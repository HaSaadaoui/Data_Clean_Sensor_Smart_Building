# Import des librairies 
from azure.cosmos import CosmosClient, exceptions
from collections import OrderedDict
#from dotenv import load_dotenv
from datetime import datetime, timedelta
import subprocess
import json
import uuid  
import os


# initialisation des variables 
    # Variable globale pour le capteur AirSensor
last_temperature = None
last_humidity = None
last_CO2 = None
last_COV = None
    #Variable globale pour le capteur Comsumption
previous_presentvalue = None
previous_timestamp = None
    #Donnée à insérer dans le conteneur
item_to_insert = {}


# Fonction permettant de rédiger les requêtes SQL pour récupérer les données brutes d'un capteur 
def query (complete_query) :
    """ Requête qui prend en entrée toute la requête SQL complète.
        Exemple : complete_query = SELECT * FROM c WHERE c.device = "a" """
    return f"'{complete_query}'" 

def round_timestamp(timestamp_str):
    """Function to round the timestamp to the nearest 10 minutes."""
    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    if timestamp.minute % 10 >= 5:
        rounded_timestamp = timestamp + timedelta(minutes=(10 - timestamp.minute % 10))
    else:
        rounded_timestamp = timestamp.replace(minute=(timestamp.minute // 10) * 10, second=0, microsecond=0)
    rounded_timestamp = rounded_timestamp.replace(second=0, microsecond=0)
    return rounded_timestamp.isoformat() + 'Z'

# Création de nouvelle données à partir du capteur Comsumption si l'information 
# 'presentvalue' existe dans le JSON
def Consumption_update(item_to_insert):
    """ Fonction spécifique au capteur Comsumption et permet de faire des retraitements.
    Si dans le fichiers json extrait il existe le champs "presentvalue" alors des opérations
    sont faites pour calculer les zones "Consumption" et "Duration". """
    global previous_presentvalue, previous_timestamp

    # Vérifiez l'existence de la clé 'presentvalue'
    current_presentvalue = item_to_insert['values'].get('presentvalue')
    if current_presentvalue is not None:
        if previous_presentvalue is not None:
            consumption = current_presentvalue - previous_presentvalue
        else:
            consumption = 0  # Pas de consommation pour le premier enregistrement

        item_to_insert['values']['Consumption'] = consumption
        previous_presentvalue = current_presentvalue

        # Convertir le champ ReceivedTimeStamp en objet datetime
        timestamp = datetime.fromisoformat(item_to_insert['ReceivedTimeStamp'].replace('Z', '+00:00'))
        # Calculer la durée en utilisant ReceivedTimeStamp
        if previous_timestamp is not None:
            duration = round((timestamp - previous_timestamp).total_seconds())
        else:
            duration = 0  # Pas de durée pour le premier enregistrement

        item_to_insert['values']['Duration'] = duration
        previous_timestamp = timestamp

        # Afficher les valeurs intermédiaires pour le débogage
        print(f"Timestamp: {timestamp}, Previous Timestamp: {previous_timestamp}, Duration: {duration}")

        # Réorganiser les clés pour placer 'Consumption' et 'Duration' après 'presentvalue'
        values = item_to_insert['values']
        reordered_values = {
            "presentvalue": values["presentvalue"],
            "Consumption": values["Consumption"],
            "Duration": values["Duration"]
        }
        item_to_insert['values'] = reordered_values
    else:
        print(f"Clé 'presentvalue' manquante dans l'élément : {item}")


# Fonction pour supprimer des champs 
def remove_data(item,list):
    """ Permet de sumprimer dans le dictionnaires un ensemble de champs qui 
    sont passés en paramètres sous forme de liste. """
    #Supprimer les champs inutiles si ils existent
    for field in list:
        item.pop(field, None)

# Code propre au Capteur Air
def decode_frame(raw_data, timestamp):
    """Fonction permettant de décoder les trames du capteur Air. """
    # Définir les arguments de la commande
    command = [
        "python",
        #"C:/Users/Codec-Report-Batch-Python-main/br_uncompress.py",
        "C:/Users/elvireperla.dakengke/Downloads/DecodePayload_AirSensor-dev/DecodePayload_AirSensor-dev/br_uncompress.py",
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


# Fonction pour remplacer les valeurs nulles par la dernière valeur connue 
def replace_nulls_with_last_value(document):
            """ Fonction propre au capteur Air. """
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


# Fonction permettant de faire des transformation adaptée au capteur Air
def Air_update(item):
    """ Fonction spécifique au capteur Air et permet de faire des retraitements.
    Si dans le fichiers json extrait il existe le champs "presentvalue" alors des opérations
    sont faites pour calculer les zones "Consumption" et "Duration". """
    global item_to_insert
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
        remove_data(decoded_result, ['batch_absolute_timestamp'])
        
        # Préparer un dictionnaire pour regrouper les données par RoundReceivedTimeStamp
        grouped_data = {}

        # Parcourir le dataset 
        for data_point in decoded_result.get('dataset', []):
            # Suppression du champ data_relative_timestamp
            remove_data(data_point, ['data_relative_timestamp'])            
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
            
            item_to_insert = replace_nulls_with_last_value(value)
            item_to_insert['id'] = str(uuid.uuid4()) 
            # Afficher l'élément à insérer
            print(f"Document à insérer pour {key} : {json.dumps(item_to_insert, indent=4)}")
    else:
        print("Erreur lors du décodage de la trame brute")

#Fonction permettant de faire le retraitement qui est adapté à chacun des capteurs
def cleaning_sensor(SensorName, item) : 
    """ Fonction finale de retraitement de tous les capteurs. """
    global item_to_insert
    if SensorName in ["Consumption", "Desk", "Light", "Son", "TempEx"] : 
        # Créer une copie pour ne pas modifier l'original
        item_to_insert = item.copy()  
        #Supprimer les champs inutiles 
        remove_data(item_to_insert, ['raw', 'HandledTimeStamp', 'humidity', 'temperature', 'battery'])
        # Arrondir le champ ReceivedTimeStamp et ajouter RoundReceivedTimeStamp
        item_to_insert['RoundReceivedTimeStamp'] = round_timestamp(item_to_insert['ReceivedTimeStamp'])
        # Réorganiser les clés pour placer 'RoundReceivedTimeStamp' après 'ReceivedTimeStamp'
        reordered_item = {}
        for key in item_to_insert:
            reordered_item[key] = item_to_insert[key]
            if key == 'ReceivedTimeStamp':
                reordered_item['RoundReceivedTimeStamp'] = item_to_insert['RoundReceivedTimeStamp']

        item_to_insert = reordered_item

        if SensorName == "Consumption" :
            # Trier les éléments par ReceivedTimeStamp
            items.sort(key=lambda x: x['ReceivedTimeStamp'])
            # Créer aléatoirement un id
            item_to_insert['id'] = str(uuid.uuid4())  # Génère un UUID comme identifiant
            # Réorganiser les clés pour placer 'RoundReceivedTimeStamp' après 'ReceivedTimeStamp'
            reordered_item = {}
            for key in item_to_insert:
                reordered_item[key] = item_to_insert[key]
                if key == 'ReceivedTimeStamp':
                    reordered_item['RoundReceivedTimeStamp'] = item_to_insert['RoundReceivedTimeStamp']

            item_to_insert = reordered_item
            Consumption_update(item_to_insert)
            # Afficher l'élément à insérer
            print(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
        elif SensorName in ["Desk", "Light", "TempEx"]:
            # Créer aléatoirement un id
            item_to_insert['id'] = str(uuid.uuid4())  # Génère un UUID comme identifiant
            # Afficher l'élément à insérer
            print(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
        elif SensorName == "Son":
            # Afficher l'élément à insérer
            print(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
        # Insérer directement l'élément dans le conteneur de destination
            try:
                destination_container.upsert_item(item_to_insert)
                print(f"Élément inséré dans DataCleanCosmos : {item_to_insert['id']}")
            except exceptions.CosmosHttpResponseError as e:
                print(f"Erreur lors de l'insertion dans DataCleanCosmos : {e.message}")
        else :
            pass
        
    elif SensorName == "Air" :
       Air_update(item)
       #Insérer le document dans le conteneur de destination
       destination_container.upsert_item(item_to_insert)
       print(f"Données insérées dans le conteneur 'DataCleanCosmos': {item_to_insert}")
       
    


# Connexion à Cosmos DB
CONNECTION_STRING = 'AccountEndpoint=https://cosmosdb-paris-chateaudun.documents.azure.com:443/;AccountKey=WQjxv3e4hNIOv2I91oLlTObR0PFVSSk3iB7goLJAMLwMEVCzgl98fmueRQEkwxBvqgmyBUmpXnfFACDbeUnUpg==;'

client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')

# Conteneur source (données brutes)
sensor_container = database.get_container_client('SensorData')

# Conteneur de destination pour les données nettoyées
destination_container = database.get_container_client('TestDataClean')

# Liste des requêtes SQL par capteur our récupérer toutes les données

sensor_query = {
    "Air" : 'SELECT c.raw, c.ReceivedTimeStamp, c.metadata FROM c WHERE c.device = "Air_05-01" AND STARTSWITH(c.ReceivedTimeStamp, "2024-12-11")' ,
    "Consumption" : 'SELECT * FROM c WHERE c.device = "LT_05-01" AND STARTSWITH(c.ReceivedTimeStamp, "2024-12-11")',
    "Desk" : 'SELECT * FROM c WHERE c.device in ("Desk_05-01", "Desk_05-02", "Desk_05-03", "Desk_05-04","Desk_05-05", "Desk_05-06", "Desk_05-07", "Desk_05-08", "Desk_05-09", "Desk_05-10", "Desk_05-11", "Desk_05-12","Desk_05-13", "Desk_05-14", "Desk_05-15", "Desk_05-16", "Desk_05-17", "Desk_05-18", "Desk_05-19", "Desk_05-20", "Desk_05-21", "Desk_05-22", "Desk_05-23", "Desk_05-24") AND STARTSWITH(c.ReceivedTimeStamp, "2024-12-11")',
    "Light" : 'SELECT * FROM c WHERE c.device in ("Light_05-02","Light_05-01") AND STARTSWITH(c.ReceivedTimeStamp, "2024-12-11")',
    "Son" : 'SELECT * FROM c WHERE c.device IN ("Son_05-01", "Son_05-02") AND STARTSWITH(c.ReceivedTimeStamp, "2024-12-11")',
    "TempEx" : 'SELECT * FROM c WHERE c.device = "TempEx_05-01" AND STARTSWITH(c.ReceivedTimeStamp, "2024-12-11")'
}

# sensor_query = {
#     "TempEx" : 'SELECT * FROM c WHERE c.device = "TempEx_05-01" and STARTSWITH(c.ReceivedTimeStamp, "2024-12-11")'
# }


# On boucle sur chacun des capteurs qu'on récupère à partir des requêtes contenues dans sensor_query
for sensor in sensor_query:
    #b = 0
    print(f"{sensor}")
    name = input("Enter your OK: ")
    # Récupérer les données des différents capteurs
    items = list(sensor_container.query_items(sensor_query[sensor], enable_cross_partition_query=True))
    # vérrifier si des données ont pu être récupérées à partir de la requête 
    if items:
        print("Données trouvées dans le conteneur SensorData :")
        for item in items:
            # Application de la fonction de retraitement des capteurs 
            cleaning_sensor(sensor, item)
            #b= b+1       

    else :
        print("Aucune donnée trouvée pour les autres capteurs dans le conteneur SensorData.")