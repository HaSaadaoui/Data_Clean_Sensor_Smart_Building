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

@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=False,
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



    # Date de début d'execution de la fonction
    date_debut = datetime(2025, 1, 17, 1, 0, 0)
    # heure locale du pc portable
    heure_locale = datetime.now()
    # Retrait de x heure à actuelle d'Azure function
    heure_lancement = heure_locale - timedelta(hours = 1)

    logging.info(f"Date et heure de lancement : {heure_lancement.isoformat()}")

    # Ecriture de la requête d'extraction de nouveaux fichiers rajoutés dans le conteneur source
    query_r = "SELECT * FROM c WHERE c.device = 'TempEx_05-01' and c.ReceivedTimeStamp > @heure_lancement "
    logging.info(f"Affichage de la requête : {query_r}")
    # Exécution de la requête pour récupérer les documents dans le conteneur source
    items = list(sensor_container.query_items(
        query = query_r,
        parameters=[{'name': '@heure_lancement', 'value': heure_lancement.isoformat()}],
        enable_cross_partition_query=True
    ))
    
    # Début d'insertion des données ssi la contion date de début de lancement est respecté
    if heure_locale >= date_debut :
        # Vérification de l'existence de documents 
        if items:
            logging.info(f"Documents récupérés : {len(items)}")
            logging.info(f"Documents récupérés : {items}")

            # On fait une boucle sur chacun des documents récupérés 
            for item in items:

                # logging.info(f"Affichage du device de l'item : {item["device"]}")
                # logging.info("Execution du cleaning ")
                # Obligation de spliter cette opération de création de la variable device afin qu'Azure 
                # puisse l'executer, sinon ERROR
                device1 = item["device"]
                device2 = device1.split('_')
                device = device2[0]
                cleaning_sensor(device, item, destination_container)
                # logging.info(f"Élément à insérer : {json.dumps(item_to_insert, indent=4)}")
                # logging.info("Cleaning terminé.")
            
        else :
            logging.info("Aucun document trouvé")
    else :
        logging.info("Condition de lancement de la fonction Azure (date et heure) pas rempli")
            