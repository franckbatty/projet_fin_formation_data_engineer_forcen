# == COUCHE BRONZE ==
"""
Cette couche bronze est la première étape du pipeline ETL pour le projet MacroBus .
Elle importe les données brutes (CSV) et les stocke dans PostgreSQL sans transformation.
"""

# === Importation des bibliothèques nécessaires ===
import psycopg2              # Connexion et requêtes PostgreSQL
import os                    # Gestion des chemins et fichiers
import pandas as pd          # Manipulation des données tabulaires
import sys                   # Gestion du PYTHONPATH
from datetime import datetime # Gestion des dates
from dotenv import load_dotenv # Permet de charger les variables d'environnement depuis un fichier .env
# Charger les variables depuis le fichier .env
load_dotenv()
 
# Ajout du chemin du package ETL
sys.path.append("/opt/airflow/ETL") # C'est le chemin du package ETL dans le conteneur Docker

# Chemins du fichier CSV
car_prices_file = "/opt/airflow/data/car_prices.csv" # c'est le chemin du fichier CSV dans le conteneur Docker
 

# Paramètres de connexion centralisés
DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB"),       # Base définie dans .env
    "user": os.getenv("POSTGRES_USER"),       # Utilisateur défini dans .env
    "password": os.getenv("POSTGRES_PASSWORD"), # Mot de passe défini dans .env
    "host": os.getenv("POSTGRES_HOST"),       # Host défini dans .env (service Docker)
    "port": os.getenv("POSTGRES_PORT")        # Port défini dans .env
}

# === Fonctions utilitaires ===
def start_message():
    print("=== DÉBUT DU DAG ===")

def end_message():
    print("=== FIN DU DAG ===")

# === Fonction EXTRACT ===
def extract_data(ti, **kwargs):
    """
    Extractions des données à partir du fichier CSV.
    Vérifions l'existence du fichier et extrons les données dans des DataFrames pandas.
    """
    try:
        print("Début d'extraction du fichiers CSV...")

        # Vérifions que le fichiers existe
        for file in [car_prices_file]:
            if not os.path.exists(file):
                raise FileNotFoundError(f"Le fichier {file} est introuvable.")

        # Charger les fichiers CSV
        car_prices_file_df = pd.read_csv(car_prices_file)

        print(f"Prix Vehicule : {car_prices_file_df.shape}") 
        print(f"Prix Vehicule : {car_prices_file_df.head(5)}")
        # Stocker dans XCom, pour que la tâche LOAD puisse y accéder vu que le format de données est un 
        # DataFrame pandas, on le convertit en dictionnaire pour le stocker dans XCom.
        # Le type de données partagées en XCom doit être sérialisable (comme un dictionnaire, une liste, etc.)
        # Et ici on utilise le dictionnaire pour stocker les DataFrames extraits.
        extracted_data = {  
            "cars": car_prices_file_df.to_dict()
        }
        ti.xcom_push(key="extracted_data", value=extracted_data)

        print("✅ Extraction réussie.")
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {str(e)}")
        raise e

# === Fonction LOAD ===
def loading_data(ti):
    print("Début du chargement dans PostgreSQL...")

    # Récupérer les données extraites
    extracted_data = ti.xcom_pull(task_ids="EXTRACT", key="extracted_data")

    # Créer des DataFrames
    car_prices_file_df = pd.DataFrame(extracted_data["cars"])
    print(f"Affichage de la taille des données recuperées : {car_prices_file_df.shape}")

    # Conversion en tuples
    # On convertit le DataFrame en liste de tuples pour l'insertion dans PostgreSQL.
    # Chaque tuple représente une ligne de données à insérer dans la table.
    # La données envoyée à PostgreSQL doit être sous forme de tuples pour l'insertion.
    car_prices_file_data = [tuple(row) for row in car_prices_file_df.itertuples(index=False, name=None)]

    # Connexion PostgreSQL
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("Connexion PostgreSQL réussie.")

    try:
        # Vidage des tables
        for table in [
            "macrobus.vehicules"
        ]: 
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            print(f"Table {table} vidée.")
        conn.commit()

        # Insertion ordonnée
        cursor.executemany("""
            INSERT INTO macrobus.vehicules 
            (year, make, model, trim, body, transmission, vin, state, condition, odometer, color, interior, seller, mmr, saledate)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s);
        """, car_prices_file_data)
        conn.commit()


        print("✅ Chargement terminé avec succès.")
    except Exception as e:
        print(f"❌ Erreur lors du chargement : {str(e)}")
        conn.rollback() 
        raise e
    finally:
        cursor.close()
        conn.close()
        print("Connexion PostgreSQL fermée.")
