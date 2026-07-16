## == Pipeline ETL pour le projet Macrobus couche Bronze avec Airflow et Python == ##
# Ce script définit un DAG (Directed Acyclic Graph) pour exécuter un pipeline EL (Extract, Load) pour le projet Macrobus.

# === Importation des modules nécessaires ===
import sys
sys.path.append("/opt/airflow/ETL")  # on force Python à inclure le dossier ETL

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# === Importation des fonctions ETL depuis le package ETL ===
try:
    from ETL.el_macrobus_bronze import (
        start_message,
        extract_data,
        loading_data,
        end_message
    )
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(f"Erreur lors de l'importation des modules ETL : {e}")

# === Paramètres par défaut du DAG ===
default_args = {
    "owner": "franck_BATTY", # Nom de l'utilisateur ou du propriétaire du DAG
    "depends_on_past": False, # Indique si l'exécution du DAG dépend des exécutions passées
    "email_on_failure": False, # Indique si un email doit être envoyé en cas d'échec du DAG
    "email_on_retry": False, # Indique si un email doit être envoyé en cas de nouvelle tentative du DAG
    "retries": 1, # Nombre de tentatives en cas d'échec du DAG
    "retry_delay": timedelta(minutes=5), # Délai entre les tentatives en cas d'échec du DAG
}

# === Définition du DAG Bronze ===
with DAG(
    "Projet_Macrobus_ETL_Bronze", # Nom du DAG
    default_args=default_args,
    description="DAG Bronze pour le pipeline ETL du projet Macrobus",
    schedule_interval="@daily", # Intervalle de planification du DAG (ici, tous les jours)
    start_date=datetime(2026, 7, 15),
    catchup=False, # Indique si le DAG doit rattraper les exécutions manquées
    tags=['Bronze', 'Macrobus', 'EL'] # Tags pour catégoriser le DAG
) as dag:

    start = PythonOperator(
        task_id="START",
        python_callable=start_message
    )

    extract = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_data,
        op_kwargs={
            "car_prices_file": "C:/Users/FRANCK BATTY/OneDrive/Desktop/projet_fin_formation_data_engineer_forcen/data/car_prices.csv" # Chemin du fichier CSV à extraire
        }
    )

    load = PythonOperator(
        task_id="LOAD",
        python_callable=loading_data
    )

    end = PythonOperator(
        task_id="END",
        python_callable=end_message
    )

    # === Définition de la tâche pour déclencher le DAG Silver === 
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="Projet_Macrobus_ETL_Silver",
        conf={
            "source": "bronze",
            "date_traitement": "{{ ds }}",
            "bronze_termine": True
        },
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    start >> extract >> load >> end >> trigger_silver
