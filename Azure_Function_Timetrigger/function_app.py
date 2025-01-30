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

@app.timer_trigger(schedule="0,10,20,30,40,50 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def insert_cleanedData_in_DataCleanCosmos(myTimer: func.TimerRequest) -> None:

# Récupérer la chaîne de connexion Cosmos DB et le nom du conteneur depuis les paramètres d'application
    cosmos_db_connection_string = os.getenv("CONNECTION_STRING")
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


    # Heure Azure
    Azure_local_time = datetime.now(timezone.utc)
    # Retirer le fuseau horaire pour le rendre en datetime naive
    Azure_local_time = Azure_local_time.replace(tzinfo=None)
    Azure_local_time_delta = Azure_local_time - timedelta(minutes=10)
   
    logging.info(f"Dernière date de lancement Azure : {Azure_local_time.isoformat()}")
    
    # Définition de la date de début d'execution de la fonction
    date_debut = datetime(2025, 1, 28, 00, 00, 0)
    # heure locale du pc portable
    heure_locale = datetime.now()

    # Ecriture de la requête d'extraction de nouveaux fichiers rajoutés dans le conteneur source
    query_r = "SELECT * FROM c WHERE c.ReceivedTimeStamp >= @Azure_local_time_delta and c.ReceivedTimeStamp<= @Azure_local_time"
    logging.info(f"Affichage de la requête : {query_r}")
    # Exécution de la requête pour récupérer les documents dans le conteneur source
    items = list(sensor_container.query_items(
        query = query_r,
        parameters=[{'name': '@Azure_local_time', 'value': Azure_local_time.isoformat()},
                    {'name': '@Azure_local_time_delta', 'value': Azure_local_time_delta.isoformat()}],
        enable_cross_partition_query=True
    ))

    
    # Début d'insertion des données ssi la condition date de début de lancement est respecté
    if heure_locale >= date_debut :
        # Vérification de l'existence de documents 
        if items:
            logging.info(f"Documents récupérés : {len(items)}")
            # logging.info(f"Documents récupérés : {items}")

            # On fait une boucle sur chacun des documents récupérés 
            for item in items:

                # Obligation de spliter cette opération de création de la variable device afin qu'Azure 
                # puisse l'executer, sinon ERROR
                device1 = item["device"]
                device2 = device1.split('_')
                device = device2[0]
                # Cleaning de des items en utilisant les fonctions contenues dans Data_Clean_merge.py
                cleaning_sensor(device, item, destination_container)
            
        else :
            logging.info("Aucun document trouvé")
    else :
        logging.info("Condition de lancement de la fonction Azure (date et heure) pas rempli")
            