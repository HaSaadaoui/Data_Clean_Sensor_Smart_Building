from azure.cosmos import CosmosClient
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

# Connexion Cosmos DB
CONNECTION_STRING = os.getenv('CLE_COSMOS')
client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')
container = database.get_container_client('SensorData')

# Requête SQL pour récupérer les données du champ 'raw' pour le capteur 'Air_05-01'
query = 'SELECT * FROM c WHERE c.device = "Son_05-02" AND STARTSWITH(c.ReceivedTimeStamp, "2024-10-10")'

# Exécution de la requête
items = list(container.query_items(query=query, enable_cross_partition_query=True))  

 # Affichage du nombre de trames trouvées
print(f"Nombre de payloads trouvés : {len(items)}")

# Affichage des résultats
for item in items:
    print(items)
