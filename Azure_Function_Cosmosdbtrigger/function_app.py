import logging
import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
import json
# import pytz
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



    # Date de début d'execution de la fonction
    date_debut = datetime(2025, 1, 29, 0, 0, 0)
    # heure locale du pc portable
    heure_locale = datetime.now()


    
    # Début d'insertion des données ssi la contion date de début de lancement est respecté
    if heure_locale >= date_debut :
        for item in azcosmosdb :

            logging.info(f"Documents récupérés : {len(item)}")
    #         # logging.info(f"Documents récupérés : {item['id']}")

    #         logging.info(f"Affichage du device de l'item : {item["device"]}")
    # #         # outputDocument.set(item)
    # #         # logging.info("Execution du cleaning ")
    # #         # Obligation de spliter cette opération de création de la variable device afin qu'Azure 
    # #         # puisse l'executer, sinon ERROR
            device1 = item["device"]
            device2 = device1.split('_')
            device = device2[0]
            logging.info(f"device : {device}")
            cleaning_sensor(device, item, destination_container)
    # #         logging.info(f"item_to_insert : {type(item_to_insert)}")
    # #         logging.info(f"item : {type(item)}")
    # #         if device != "Air" :
    # #             outputDocument.set(func.Document(item_to_insert))
    # #         elif device == "Air":
                
    # #             # logging.info(f" dictionnaire 1 : {list_item_to_insert[0]}")
    # #             for dictionnary in list_item_to_insert:
    # #                 outputDocument.set(func.Document(dictionnary))
    # #         #         logging.info(f" type de dictionnary {type(dictionnary)}")
    # #         # outputDocument.set(item_to_insert)
    # #             # logging.info(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
    # #             # logging.info("Cleaning terminé.")
            
    else :
        logging.info("Condition de lancement de la fonction Azure (date et heure) pas rempli")
