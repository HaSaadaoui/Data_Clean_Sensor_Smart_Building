from azure.cosmos import CosmosClient
from collections import OrderedDict
#from dotenv import load_dotenv
from datetime import datetime, timedelta
import subprocess
import json
import uuid  
import os


# Variables globales pour stocker les dernières valeurs
last_temperature = None
last_humidity = None
last_CO2 = None
last_COV = None

# Récupérer les variables d'environnement du fichier .env
#load_dotenv()  

# Connexion à Cosmos DB
CONNECTION_STRING = 'AccountEndpoint=https://cosmosdb-paris-chateaudun.documents.azure.com:443/;AccountKey=WQjxv3e4hNIOv2I91oLlTObR0PFVSSk3iB7goLJAMLwMEVCzgl98fmueRQEkwxBvqgmyBUmpXnfFACDbeUnUpg==;'

client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')

# Conteneur source (données brutes)
sensor_container = database.get_container_client('SensorData')# Requête SQL pour récupérer les données brutes du capteur 'Air_05-01' pour une journée spécifique
query = 'SELECT c.raw, c.ReceivedTimeStamp, c.metadata FROM c WHERE c.device = "Air_05-01" AND c.ReceivedTimeStamp >= "2024-11-21"'
#query = 'SELECT c.raw, c.ReceivedTimeStamp, c.metadata FROM c WHERE c.device = "Air_05-01" AND c.ReceivedTimeStamp >= "2024-01-01"'

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

def round_timestamp(timestamp_str):
    """Function to round the timestamp to the nearest 10 minutes."""
    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    if timestamp.minute % 10 >= 5:
        rounded_timestamp = timestamp + timedelta(minutes=(10 - timestamp.minute % 10))
    else:
        rounded_timestamp = timestamp.replace(minute=(timestamp.minute // 10) * 10, second=0, microsecond=0)
    rounded_timestamp = rounded_timestamp.replace(second=0, microsecond=0)
    return rounded_timestamp.isoformat() + 'Z'
        
# Fonction pour remplacer les valeurs nulles par la dernière valeur connue
def replace_nulls_with_last_value(document):
            global last_temperature, last_humidity, last_CO2, last_COV
            
            # Remplacer la température si elle est nulle
            if document["values"]["temperature"] is None:
                document["values"]["temperature"] = last_temperature
            else:
                last_temperature = document["values"]["temperature"]
            
            # Remplacer l'humidité si elle est nulle
            if document["values"]["humidity"] is None:
                document["values"]["humidity"] = last_humidity
            else:
                last_humidity = document["values"]["humidity"]
            
            # Remplacer le CO2 si il est nul
            if document["values"]["CO2"] is None:
                document["values"]["CO2"] = last_CO2
            else:
                last_CO2 = document["values"]["CO2"]
            
            # Remplacer les COV si il est nul
            if document["values"]["COV"] is None:
                document["values"]["COV"] = last_COV
            else:
                last_COV = document["values"]["COV"]
            return document

# Parcourir et traiter chaque trame brute
for item in items:
    raw_data = item['raw']
    received_timestamp = item['ReceivedTimeStamp']  # Récupérer ReceivedTimeStamp
    trimmed_timestamp = received_timestamp[:23] + "Z"  # Garder jusqu'à 3 chiffres de millisecondes et ajouter "Z"
    print(f"Trame brute : {raw_data}")
    print(f"TimeStamp : {received_timestamp}")
    print(f"trimmed_timestamp : {trimmed_timestamp }")

    # Décoder la trame brute
    decoded_result = decode_frame(raw_data, trimmed_timestamp)

    if decoded_result:
        print("Résultat décodé avant modification des labels :")
        print(json.dumps(decoded_result, indent=4))
        # Suppression du champ batch_absolute_timestamp
        if 'batch_absolute_timestamp' in decoded_result:
            del decoded_result['batch_absolute_timestamp']
        
        # Préparer un dictionnaire pour regrouper les données par RoundReceivedTimeStamp
        grouped_data = {}

        # Parcourir le dataset 
        for data_point in decoded_result.get('dataset', []):
            # Suppression du champ data_relative_timestamp
            if 'data_relative_timestamp' in data_point:
                del data_point['data_relative_timestamp']            
            # Renommer les labels et conversion des valeurs
            if data_point['data']['label_name'] == 'temperature':
                data_point['data']['value'] = round(data_point['data']['value'] * 0.01, 1)
            elif data_point['data']['label_name'] == 'humidity':
                data_point['data']['value'] = data_point['data']['value'] * 0.01
            
            # Calculer le RoundReceivedTimeStamp
            round_received_timestamp = round_timestamp(data_point['data_absolute_timestamp'])
            
            # Initialiser le dictionnaire pour ce RoundReceivedTimeStamp s'il n'existe pas
            if round_received_timestamp not in grouped_data:
                grouped_data[round_received_timestamp] = {
                    "values": item.get(data_point['data']['value'], {}),  
                    "device": "Air_05-01",
                    "deveui": "70B3D5E75E01C1FB",
                    "ReceivedTimeStamp": data_point['data_absolute_timestamp'],
                    "RoundReceivedTimeStamp": round_received_timestamp,
                    "metadata": item.get('metadata', {})
                }
            
            label_name = data_point['data']['label_name']
            grouped_data[round_received_timestamp]["values"][label_name] = data_point['data']['value']
       
        # Afficher les documents avant l'insertion
        for key, value in grouped_data.items():
            value['values'].setdefault('temperature', None)  # ou utilisez une valeur par défaut comme 0.0
            value['values'].setdefault('humidity', None)     # ou utilisez une valeur par défaut comme 0.0
            value['values'].setdefault('CO2', None)          # ou utilisez une valeur par défaut comme 0.0
            value['values'].setdefault('COV', None)          # ou utilisez une valeur par défaut comme 0.0
        # Créer un OrderedDict avec les clés dans l'ordre spécifié
            ordered_values = OrderedDict([
                    ('temperature', value['values']['temperature']),
                    ('humidity', value['values']['humidity']),
                    ('CO2', value['values']['CO2']),
                    ('COV', value['values']['COV']),
            ])
            # Remplacer la partie "values" par l'OrderedDict réorganisé
            value['values'] = ordered_values
            
            value = replace_nulls_with_last_value(value)
            print(f"Document à insérer pour {key} : {json.dumps(value, indent=4)}")
            #Insérer le document dans le conteneur de destination
            value['id'] = str(uuid.uuid4())  
            destination_container.upsert_item(value)
            print(f"Données insérées dans le conteneur 'DataCleanCosmos': {value}")
    else:
        print("Erreur lors du décodage de la trame brute")