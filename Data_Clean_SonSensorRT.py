from azure.cosmos import CosmosClient, exceptions
from datetime import datetime, timedelta
import json

# Connexion à Cosmos DB
CONNECTION_STRING = 'AccountEndpoint=https://cosmosdb-paris-chateaudun.documents.azure.com:443/;AccountKey=WQjxv3e4hNIOv2I91oLlTObR0PFVSSk3iB7goLJAMLwMEVCzgl98fmueRQEkwxBvqgmyBUmpXnfFACDbeUnUpg==;'
client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')
container = database.get_container_client("SensorData")

# Conteneur de destination pour les données nettoyées
destination_container = database.get_container_client('DataCleanCosmos')

# Requête SQL pour récupérer toutes les données avec device = "Son_05-01" ou "Son_05-02"
device_query = 'SELECT * FROM c WHERE c.device IN ("Son_05-01", "Son_05-02") AND c.ReceivedTimeStamp >= "2024-11-18T00:00:00Z"'

def clean_item(item):
    """Function to clean unnecessary fields from the item."""
    fields_to_remove = ['HandledTimeStamp', 'raw', 'humidity', 'temperature', 'battery']
    for field in fields_to_remove:
        if field in item:
            del item[field]
    return item

def round_timestamp(timestamp_str):
    """Function to round the timestamp to the nearest 10 minutes."""
    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    if timestamp.minute % 10 >= 5:
        rounded_timestamp = timestamp + timedelta(minutes=(10 - timestamp.minute % 10))
    else:
        rounded_timestamp = timestamp.replace(minute=(timestamp.minute // 10) * 10, second=0, microsecond=0)
    rounded_timestamp = rounded_timestamp.replace(second=0, microsecond=0)
    return rounded_timestamp.isoformat() + 'Z'

try:
    # Récupérer les données existantes
    existing_items = list(container.query_items(device_query, enable_cross_partition_query=True))

    if existing_items:
        print(f"{len(existing_items)} éléments trouvés avec les devices 'Son_05-01' et 'Son_05-02' dans DataCleanCosmos.")

        for item in existing_items:
            # Copier l'élément et le nettoyer
            item_to_update = clean_item(item.copy())

            # Arrondir le champ ReceivedTimeStamp et ajouter RoundReceivedTimeStamp
            item_to_update['RoundReceivedTimeStamp'] = round_timestamp(item_to_update['ReceivedTimeStamp'])

            # Réorganiser les clés pour placer 'RoundReceivedTimeStamp' après 'ReceivedTimeStamp'
            reordered_item = {}
            for key in item_to_update:
                reordered_item[key] = item_to_update[key]
                if key == 'ReceivedTimeStamp':
                    reordered_item['RoundReceivedTimeStamp'] = item_to_update['RoundReceivedTimeStamp']

            item_to_update = reordered_item

            # Afficher l'élément à insérer
            print(f"Élément à insérer : {json.dumps(item_to_update, indent=4)}")

             # Mettre à jour ou insérer l'élément dans le conteneur
            try:
                 destination_container.upsert_item(item_to_update)  # No partition_key argument
                 print(f"Élément mis à jour dans DataCleanCosmos : {item_to_update['id']}")
            except exceptions.CosmosHttpResponseError as e:
                 if e.status_code == 412:  # Concurrency conflict error (etag mismatch)
                     print(f"Conflit de mise à jour détecté pour l'élément {item_to_update['id']}.")
                 else:
                     print(f"Erreur lors de la mise à jour de l'élément {item_to_update['id']} : {e.message}")
    else:
        print("Aucun élément trouvé pour mise à jour avec les devices.")
except exceptions.CosmosHttpResponseError as e:
    print(f"Erreur lors de la requête Cosmos DB : {e.message}")
