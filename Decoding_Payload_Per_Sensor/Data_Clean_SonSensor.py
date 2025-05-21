from azure.cosmos import CosmosClient, exceptions
import json
from datetime import datetime
import uuid  
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

# Connexion à Cosmos DB
CONNECTION_STRING = os.getenv('CLE_COSMOS')
client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')

# Conteneur source (données brutes)
sensor_container = database.get_container_client('SensorData')

# Conteneur de destination pour les données nettoyées
destination_container = database.get_container_client('DataCleanCosmos')

# Requête SQL pour récupérer toutes les données des autres capteurs
other_sensors_query = 'SELECT * FROM c WHERE c.device = "Son_05-01" AND STARTSWITH(c.ReceivedTimeStamp, "2024-10-10")'

try:
    # Récupération des données du premier conteneur
    items = list(sensor_container.query_items(other_sensors_query, enable_cross_partition_query=True))

    if items:
        print("Données trouvées dans le conteneur SensorData :")
        for item in items:
            # Créer une copie de l'élément et ajouter le champ 'Type'
            item_to_insert = item.copy()  # Créer une copie pour ne pas modifier l'original
            item_to_insert['id'] = str(uuid.uuid4())  # Génère un UUID comme identifiant
            # Afficher l'élément à insérer
            print(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
        
            # Insérer directement l'élément dans le conteneur de destination
            try:
                destination_container.upsert_item(item_to_insert)
                print(f"Élément inséré dans DataCleanCosmos : {item_to_insert['id']}")
            except exceptions.CosmosHttpResponseError as e:
                print(f"Erreur lors de l'insertion dans DataCleanCosmos : {e.message}")
    else:
        print("Aucune donnée trouvée pour les autres capteurs dans le conteneur SensorData.")
except exceptions.CosmosHttpResponseError as e:
    print(f"Erreur lors de la requête Cosmos DB : {e.message}")
