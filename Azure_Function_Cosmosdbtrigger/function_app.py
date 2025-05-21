import logging
import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
import json
import pytz
from datetime import datetime, timedelta, timezone
import constants
from Data_Clean_merge import *
import os
import uuid 
import subprocess
from collections import OrderedDict

app = func.FunctionApp()

@app.cosmos_db_trigger(arg_name="azcosmosdb", container_name="SensorData",
                        database_name="SmartBuildingDB-Paris-Chateaudun", connection="cosmosdbparischateaudun_DOCUMENTDB",
                        lease_database_name= "SmartBuildingDB-Paris-Chateaudun", create_lease_container_if_not_exists= True)  
def insert_cleanedData_in_DataCleanCosmos(azcosmosdb: func.DocumentList):
    logging.info('Python CosmosDB triggered.')

# Récupérer la chaîne de connexion Cosmos DB et le nom du conteneur depuis les paramètres d'application
    cosmos_db_connection_string = os.getenv("cosmosdbparischateaudun_DOCUMENTDB")
    container_name1 = os.getenv("CONTAINER_NAME_INPUT")
    container_name2 = os.getenv("CONTAINER_NAME_OUTPUT")
    database_name = os.getenv("DATABASE")
# Se connecter à Cosmos DB
    client = CosmosClient.from_connection_string(cosmos_db_connection_string) 
# Accéder à la base de données (nom de la base de données à spécifier)
    database = client.get_database_client(database_name)   
# Accéder aux conteneurs
    sensor_container = database.get_container_client(container_name1)
    destination_container = database.get_container_client(container_name2)


    # Cette Etape permet de tenir compte du décalage horaire entre Azure et l'heure locale
        # Par exemple si je souhaite que l'exécution débute le 29/01/2025 à minuit heure locale
        # Azure aura pour heure 23h:00 et si je vérifie la condition heure_utc >= data_debut
        # j'empêche Azure de cleaner des données de 23h:00

        # Définition de la localisation d'exécution du code
    local_tz = pytz.timezone('Europe/Paris') 
        # Définition de la date de début d'execution en heure locale souhaitée
    date_debut = datetime(2025, 1, 29, 0, 0, 0)
        # heure locale du pc portable
    heure_locale = datetime.now()
        # Convertion de l'heure locale en heure UTC
    heure_locale_avec_tz = local_tz.localize(heure_locale)
    heure_utc = heure_locale_avec_tz.astimezone(pytz.utc)


    
    # Début d'insertion des données ssi la condition date de début de lancement est respecté
    if heure_utc >= date_debut :
        for item in azcosmosdb :

            logging.info(f"Documents récupérés : {len(item)}")

            # Obligation de spliter cette opération de création de la variable device afin qu'Azure 
            # puisse l'executer, sinon ERROR
            device1 = item["device"]
            device2 = device1.split('_')
            device = device2[0]
            logging.info(f"device : {device}")
            # Execution du cleaning des données à partir du code Data_Clean_merge.py
            cleaning_sensor(device, item, destination_container)
            
    else :
        logging.info("Condition de lancement de la fonction Azure (date et heure) pas rempli")
