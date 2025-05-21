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
other_sensors_query = 'SELECT * FROM c WHERE c.device in ("TempEx_05-01") AND c.ReceivedTimeStamp >= "2024-12-10"'
#other_sensors_query = 'SELECT * FROM c WHERE c.device = "Light_05-02" AND STARTSWITH(c.ReceivedTimeStamp, "2024-10-10")'

try:
    # Récupération des données du premier conteneur
    items = list(sensor_container.query_items(other_sensors_query, enable_cross_partition_query=True))
    print(f"Nombre de JSON avant traitement : {len(items)}")
    processed_items = []  # Liste vide pour éviter le NameError

    if items:
        print("Données trouvées dans le conteneur SensorData :")
        for item in items:
            # Créer une copie de l'élément et ajouter le champ 'Type'
            item_to_insert = item.copy()  # Créer une copie pour ne pas modifier l'original
            item_to_insert['id'] = str(uuid.uuid4())  # Génère un UUID comme identifiant

            # Arrondir le champ ReceivedTimeStamp à la dizaine de minutes la plus proche
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

            # Suppression des champs inutiles          
            del item_to_insert['raw']  #  retirer le champ 'raw'
            del item_to_insert['HandledTimeStamp']  #  retirer le champ 'HandledTimeStamp'
            del item_to_insert['humidity']  #  retirer le champ 'humidity'
            del item_to_insert['temperature']  #  retirer le champ 'temperature'
            del item_to_insert['battery']  #  retirer le champ 'battery'
           # del item_to_insert['Type']  #  retirer le champ 'Type'
        
        print(f"Nombre de JSON après traitement : {len(processed_items)}")
            # Afficher l'élément à insérer
            #print(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
            
            # Insérer directement l'élément dans le conteneur de destination
            #try:
                #destination_container.upsert_item(item_to_insert)
                #print(f"Élément inséré dans DataCleanCosmos : {item_to_insert['id']}")
            #except exceptions.CosmosHttpResponseError as e:
                #print(f"Erreur lors de l'insertion dans DataCleanCosmos : {e.message}")
    else:
        print("Aucune donnée trouvée pour les autres capteurs dans le conteneur SensorData.")
except exceptions.CosmosHttpResponseError as e:
    print(f"Erreur lors de la requête Cosmos DB : {e.message}")
