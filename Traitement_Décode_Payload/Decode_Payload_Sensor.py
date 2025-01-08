from azure.cosmos import CosmosClient, exceptions
import subprocess
import json

# Fonction pour décoder une trame brute
def decode_frame(raw_data, timestamp):
    # Définir les arguments de la commande
    command = [
        "python",
        "C:/Users/Codec-Report-Batch-Python-main/br_uncompress.py",
        "-a",
        "-t", timestamp,            # Utilisation de la variable timestamp ici
        "3",                        
        "1,10,7,temperature",       # Parametre tempertarue
        "2,100,6,humidity",         # Parametre Humidity
        "3,10,6,CO2",               # Parametre CO2
        "4,10,6,COV",               # Parametre COV
        "-if",
        raw_data                    # Trame fournie par l'utilisateur
    ]


    try:
        # Exécuter la commande
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return json.loads(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors du décodage : {e}")
        print("Sortie standard :", e.stdout)
        print("Sortie d'erreur :", e.stderr)
        return None

if __name__ == "__main__":
    # Demander à l'utilisateur de saisir la trame et le timestamp
    raw_data = input("Veuillez entrer la trame à décoder : ")
    timestamp = input("Veuillez entrer le timestamp (ex : 2024-10-10T01:11:34.944Z) : ")
    
    # Décoder la trame avec le timestamp fourni
    decoded_result = decode_frame(raw_data, timestamp)
    
    # Afficher le résultat décodé
    if decoded_result:
        print("Résultat décodé :")
        print(json.dumps(decoded_result, indent=4))
