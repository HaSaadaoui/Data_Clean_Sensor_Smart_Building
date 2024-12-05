from azure.cosmos import CosmosClient, exceptions
import json
from datetime import datetime, timedelta
import uuid

# Connexion à Cosmos DB
CONNECTION_STRING = 'AccountEndpoint=https://cosmosdb-paris-chateaudun.documents.azure.com:443/;AccountKey=WQjxv3e4hNIOv2I91oLlTObR0PFVSSk3iB7goLJAMLwMEVCzgl98fmueRQEkwxBvqgmyBUmpXnfFACDbeUnUpg==;'
client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')

# Conteneur source (données brutes)
sensor_container = database.get_container_client('SensorData')

# Conteneur de destination pour les données nettoyées
destination_container = database.get_container_client('DataCleanCosmos')

# Requête SQL pour récupérer toutes les données des autres capteurs
other_sensors_query = 'SELECT * FROM c WHERE c.device = "LT_05-01" AND c.ReceivedTimeStamp >= "2024-11-18T00:00:00Z"'  # requete pour récuperer les données à partir de Janvier 2024

try:
    # Récupération des données du premier conteneur
    items = list(sensor_container.query_items(other_sensors_query, enable_cross_partition_query=True))

    if items:
        print("Données trouvées dans le conteneur SensorData :")

        # Trier les éléments par ReceivedTimeStamp
        items.sort(key=lambda x: x['ReceivedTimeStamp'])

        previous_presentvalue = None
        previous_timestamp = None

        for item in items:
            # Créer une copie de l'élément et ajouter le champ 'Type'
            item_to_insert = item.copy()  # Créer une copie pour ne pas modifier l'original
            item_to_insert['Type'] = item_to_insert.get('device')  # Ajouter le champ Type
            item_to_insert['id'] = str(uuid.uuid4())  # Génère un UUID comme identifiant

            # Convertir le champ ReceivedTimeStamp en objet datetime
            timestamp = datetime.fromisoformat(item_to_insert['ReceivedTimeStamp'].replace('Z', '+00:00'))

            # Arrondir les minutes
            if timestamp.minute % 10 >= 5:
                rounded_timestamp = timestamp + timedelta(minutes=(10 - timestamp.minute % 10))
            else:
                rounded_timestamp = timestamp.replace(minute=(timestamp.minute // 10) * 10, second=0, microsecond=0)

            # Remplacer les secondes et microsecondes par 0
            rounded_timestamp = rounded_timestamp.replace(second=0, microsecond=0)

            item_to_insert['RoundReceivedTimeStamp'] = rounded_timestamp.isoformat() + 'Z'

            # Réorganiser les clés pour placer 'RoundReceivedTimeStamp' après 'ReceivedTimeStamp'
            reordered_item = {}
            for key in item_to_insert:
                reordered_item[key] = item_to_insert[key]
                if key == 'ReceivedTimeStamp':
                    reordered_item['RoundReceivedTimeStamp'] = item_to_insert['RoundReceivedTimeStamp']

            item_to_insert = reordered_item

            # Vérifiez l'existence de la clé 'presentvalue'
            current_presentvalue = item_to_insert['values'].get('presentvalue')
            if current_presentvalue is not None:
                if previous_presentvalue is not None:
                    consumption = current_presentvalue - previous_presentvalue
                else:
                    consumption = 0  # Pas de consommation pour le premier enregistrement

                item_to_insert['values']['Consumption'] = consumption
                previous_presentvalue = current_presentvalue

                # Calculer la durée en utilisant ReceivedTimeStamp
                if previous_timestamp is not None:
                    duration = round((timestamp - previous_timestamp).total_seconds())
                else:
                    duration = 0  # Pas de durée pour le premier enregistrement

                item_to_insert['values']['Duration'] = duration
                previous_timestamp = timestamp

                # Afficher les valeurs intermédiaires pour le débogage
                print(f"Timestamp: {timestamp}, Previous Timestamp: {previous_timestamp}, Duration: {duration}")

                # Supprimer les champs inutiles si ils existent
                for field in ['raw', 'HandledTimeStamp', 'humidity', 'temperature', 'battery', 'Type']:
                    item_to_insert.pop(field, None)

                # Réorganiser les clés pour placer 'Consumption' et 'Duration' après 'presentvalue'
                values = item_to_insert['values']
                reordered_values = {
                    "presentvalue": values["presentvalue"],
                    "Consumption": values["Consumption"],
                    "Duration": values["Duration"]
                }
                item_to_insert['values'] = reordered_values

                # Afficher l'élément à insérer
                print(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
                
                # Insérer directement l'élément dans le conteneur de destination
                try:
                    destination_container.upsert_item(item_to_insert)
                    print(f"Élément inséré dans DataCleanCosmos : {item_to_insert['id']}")
                except exceptions.CosmosHttpResponseError as e:
                   print(f"Erreur lors de l'insertion dans DataCleanCosmos : {e.message}")
            else:
                print(f"Clé 'presentvalue' manquante dans l'élément : {item}")
    else:
        print("Aucune donnée trouvée pour les autres capteurs dans le conteneur SensorData.")
except exceptions.CosmosHttpResponseError as e:
    print(f"Erreur lors de la requête Cosmos DB : {e.message}")
