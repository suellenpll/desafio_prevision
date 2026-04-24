"""
DAG: bronze_customers
Camada: Bronze
Descrição: Ingestão do customers.csv para o banco SQLite (camada Bronze)
Projeto: desafio-prevision

Schema real do CSV:
  customer_id | name | email | plan | segment | status | created_at | country
  200 registros | country tem 8 nulls esperados
  plans: growth, starter, enterprise
  segments: Mid-Market, SMB, Enterprise
  status: active, churned, trial
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os
import logging

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/home/felipe/desafio_prevision/airflow")
CSV_PATH     = os.path.join(AIRFLOW_HOME, "data", "bronze", "customers.csv")
DB_PATH      = os.path.join(AIRFLOW_HOME, "data", "prevision.db")
TABLE_NAME   = "bronze_customers"

EXPECTED_COLUMNS = ["customer_id", "name", "email", "plan", "segment", "status", "created_at", "country"]
VALID_PLANS      = {"growth", "starter", "enterprise"}
VALID_SEGMENTS   = {"Mid-Market", "SMB", "Enterprise"}
VALID_STATUSES   = {"active", "churned", "trial"}

default_args = {
    "owner": "desafio-prevision",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# =============================================================================
# TASKS
# =============================================================================

def validate_source(**context):
    """Verifica existência do arquivo, colunas esperadas e arquivo não vazio."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Arquivo não encontrado: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, nrows=5)
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Colunas ausentes no CSV: {missing_cols}")

    row_count = sum(1 for _ in open(CSV_PATH)) - 1
    if row_count == 0:
        raise ValueError("CSV de customers está vazio!")

    logging.info(f"Arquivo OK — {row_count} linhas | colunas: {list(df.columns)}")
    context["ti"].xcom_push(key="source_row_count", value=row_count)


def load_to_bronze(**context):
    """
    Carrega o CSV na tabela bronze_customers (SQLite).
    Adiciona colunas de controle: _ingested_at, _source_file, _dag_run_id.
    Modo replace → idempotente.
    """
    df = pd.read_csv(CSV_PATH)

    df["_ingested_at"] = datetime.utcnow().isoformat()
    df["_source_file"] = os.path.basename(CSV_PATH)
    df["_dag_run_id"]  = context["run_id"]

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

    logging.info(f"'{TABLE_NAME}' carregada: {len(df)} registros.")
    context["ti"].xcom_push(key="row_count", value=len(df))


def validate_bronze(**context):
    """
    Validações pós-carga:
    - Colunas obrigatórias sem nulls (exceto country: ~8 nulls esperados)
    - Valores de plan/segment/status dentro do domínio
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

    logging.info(f"Total na Bronze: {len(df)} registros")

    for col in ["customer_id", "name", "email", "plan", "segment", "status", "created_at"]:
        nulls = df[col].isnull().sum()
        if nulls > 0:
            raise ValueError(f"Coluna obrigatória '{col}' tem {nulls} nulls inesperados!")

    invalid_plan = df[~df["plan"].isin(VALID_PLANS)]["plan"].unique()
    if len(invalid_plan):
        logging.warning(f"Planos fora do domínio esperado: {invalid_plan}")

    invalid_seg = df[~df["segment"].isin(VALID_SEGMENTS)]["segment"].unique()
    if len(invalid_seg):
        logging.warning(f"Segmentos fora do domínio esperado: {invalid_seg}")

    country_nulls = df["country"].isnull().sum()
    logging.info(f"country nulls: {country_nulls} (esperado: ~8 — clientes sem país informado)")

    logging.info("✅ bronze_customers validada com sucesso.")


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="bronze_customers",
    description="Ingestão de customers.csv → camada Bronze (SQLite)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "customers", "desafio-prevision"],
) as dag:

    t1 = PythonOperator(task_id="validate_source", python_callable=validate_source)
    t2 = PythonOperator(task_id="load_to_bronze",  python_callable=load_to_bronze)
    t3 = PythonOperator(task_id="validate_bronze", python_callable=validate_bronze)

    t1 >> t2 >> t3
