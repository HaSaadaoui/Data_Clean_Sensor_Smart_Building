from azure.cosmos import CosmosClient
from dotenv import load_dotenv
from datetime import datetime
import subprocess
import json
import uuid  
import os

# Récupérer les variables d'environnement du fichier .env
load_dotenv()  

# Connexion à Cosmos DB
CONNECTION_STRING = os.getenv('CLE_COSMOS')
client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')

# Conteneur source (données brutes)
sensor_container = database.get_container_client('SensorData')

# Requête SQL pour récupérer les données brutes du capteur 'Air_05-01' pour une journée spécifique
#query = 'SELECT c.raw, c.ReceivedTimeStamp, c.metadata FROM c WHERE c.device = "Air_05-01" AND STARTSWITH(c.ReceivedTimeStamp, "2024-09-30")'
query = 'SELECT c.raw, c.ReceivedTimeStamp, c.metadata FROM c WHERE c.device = "Air_05-01" AND c.ReceivedTimeStamp >= "2024-01-01"'

# Exécution de la requête pour obtenir les éléments
items = list(sensor_container.query_items(query=query, enable_cross_partition_query=True))

# Affichage du nombre de trames trouvées
print(f"Nombre de payloads trouvés : {len(items)}")

# Conteneur de destination pour les données nettoyées et décodées
destination_container = database.get_container_client('DataCleanCosmos')

def decode_frame(raw_data, timestamp):
    # Définir les arguments de la commande
    command = [
        "python",
        "C:/Users/Codec-Report-Batch-Python-main/br_uncompress.py",
        "-a",
        "-t", timestamp,            # Utilisation de la variable timestamp ici
        "3",                        # Première partie de -a
        "1,10,7,temperature",       # Parametre tempertarue
        "2,100,6,humidity",         # Parametre Humidity
        "3,10,6,CO2",               # Parametre CO2
        "4,10,6,COV",               # Parametre COV
        "-if",
        raw_data                    # Trame fournie par l'utilisateur
    ]

    try:
        # Exécuter la commande de décodage
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return json.loads(result.stdout.strip())  # Retourner le résultat décodé en format JSON
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors du décodage : {e}")
        print("Sortie standard :", e.stdout)
        print("Sortie d'erreur :", e.stderr)
        return None

# Parcourir et traiter chaque trame brute
for item in items:
    raw_data = item['raw']
    received_timestamp = item['ReceivedTimeStamp']  # Récupérer ReceivedTimeStamp
    trimmed_timestamp = received_timestamp[:23] + "Z"  # Garder jusqu'à 3 chiffres de millisecondes et ajouter "Z"
    print(f"Trame brute : {raw_data}")
    print(f"TimeStamp : {received_timestamp}")
    print(f"trimmed_timestamp : {trimmed_timestamp }")

    # Décoder la trame brute
    decoded_result = decode_frame(raw_data,trimmed_timestamp)

    if decoded_result:
        print("Résultat décodé avant modification des labels :")
        print(json.dumps(decoded_result, indent=4))
        # Suppression du champ batch_absolute_timestamp
        if 'batch_absolute_timestamp' in decoded_result:
                del decoded_result['batch_absolute_timestamp']
        # Préparer une nouvelle liste pour les données nettoyées
        cleaned_dataset = []
        # Parcourir le dataset 
        for data_point in decoded_result.get('dataset', []):
            # Suppression du champ data_relative_timestamp
            if 'data_relative_timestamp' in data_point:
                del data_point['data_relative_timestamp']            
            # Renommer les labels et conversion des valeurs
            if data_point['data']['label_name'] == 'temperature':
                data_point['data']['value'] = round(data_point['data']['value'] * 0.01, 1)
            # Renommer data_absolute_timestamp
            data_point['ReceivedTimeStamp'] = data_point.pop('data_absolute_timestamp', '')
            # Ajouter le data_point nettoyé à la nouvelle liste
            cleaned_dataset.append(data_point)
        # Mettre à jour le résultat décodé avec le dataset nettoyé
        decoded_result['dataset'] = cleaned_dataset
        # Ajouter les colonnes 'device','deveui','atchReceivedTimeStamp','metadata', et 'id'
        decoded_result['device'] = "Air_05-01"
        decoded_result['deveui'] = "70B3D5E75E01C1FB"
        decoded_result['ReceivedTimeStamp'] = item.get('ReceivedTimeStamp', {})
        decoded_result['metadata'] = item.get('metadata', {})
        decoded_result['id'] = str(uuid.uuid4())  
        # Afficher le document avant l'insertion
        print(f"Document à insérer : {json.dumps(decoded_result, indent=4)}")
        # Insérer le document dans le conteneur de destination
        destination_container.upsert_item(decoded_result)
        print(f"Données insérées dans le conteneur 'DataCleanCosmos': {decoded_result}")
    else:
        print("Échec du décodage pour cette trame.")

