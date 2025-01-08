import subprocess
import json

def decode_frame(raw_data):
    # Définir les arguments de la commande
    command = [
        "python",
        "C:/Users/Codec-Report-Batch-Python-main/br_uncompress.py",
        "-a",
        "3",           # Première partie de -a
        "1,10,7,T",    # Deuxième partie de -a
        "2,100,6,H",   # Troisième partie de -a
        "3,10,6,CO2",  # Quatrième partie de -a
        "4,10,6,COV",  # Cinquième partie de -a
        "-if",
        raw_data       # Trame fournie par l'utilisateur
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
    # Demander à l'utilisateur de saisir la trame
    raw_data = input("Veuillez entrer la trame à décoder : ")
    
    # Décoder la trame
    decoded_result = decode_frame(raw_data)
    
    # Afficher le résultat décodé
    if decoded_result:
        print("Résultat décodé :")
        print(json.dumps(decoded_result, indent=4))
