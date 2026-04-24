"""
DAG: bronze_feedbacks
Camada: Bronze
Descrição: Ingestão do feedbacks.csv para o banco SQLite (camada Bronze)
Projeto: desafio-prevision

Schema real do CSV:
  feedback_id | customer_id | type | channel | sentiment | message | created_at | nps_score | resolved
  229 registros
  customer_id: 8 nulls esperados
  nps_score: 168 nulls esperados (só preenchido quando channel = 'nps')
  resolved: 68 nulls esperados
  type: praise | bug | complaint | feature_request | support
  channel: nps | suporte | formulario | email | plataforma
  sentiment: positive | negative | neutral
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
CSV_PATH     = os.path.join(AIRFLOW_HOME, "data", "bronze", "feedbacks.csv")
DB_PATH      = os.path.join(AIRFLOW_HOME, "data", "prevision.db")
TABLE_NAME   = "bronze_feedbacks"

EXPECTED_COLUMNS = [
    "feedback_id", "customer_id", "type", "channel",
    "sentiment", "message", "created_at", "nps_score", "resolved"
]
VALID_TYPES      = {"praise", "bug", "complaint", "feature_request", "support"}
VALID_CHANNELS   = {"nps", "suporte", "formulario", "email", "plataforma"}
VALID_SENTIMENTS = {"positive", "negative", "neutral"}

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
    """Verifica existência, colunas esperadas e arquivo não vazio."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Arquivo não encontrado: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, nrows=5)
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Colunas ausentes no CSV: {missing_cols}")

    row_count = sum(1 for _ in open(CSV_PATH)) - 1
    if row_count == 0:
        raise ValueError("CSV de feedbacks está vazio!")

    logging.info(f"Arquivo OK — {row_count} linhas | colunas: {list(df.columns)}")
    logging.info(
        "NOTA: nps_score é preenchido apenas quando channel='nps' — ~168 nulls são esperados. "
        "customer_id tem ~8 nulls (feedbacks sem vínculo de cliente identificado). "
        "resolved tem ~68 nulls (feedbacks sem campo de resolução aplicável)."
    )
    context["ti"].xcom_push(key="source_row_count", value=row_count)


def load_to_bronze(**context):
    """
    Carrega o CSV na tabela bronze_feedbacks (SQLite).
    Preserva dados brutos — incluindo nulls esperados em nps_score, resolved e customer_id.
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
    - feedback_id, type, channel, sentiment, message, created_at: sem nulls
    - customer_id: ~8 nulls esperados (documentado)
    - nps_score: ~168 nulls esperados (só válido para channel='nps')
    - resolved: ~68 nulls esperados
    - nps_score quando preenchido deve estar entre 0 e 10
    - Domínios: type, channel, sentiment
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

    logging.info(f"Total na Bronze: {len(df)} registros")

    # Obrigatórias sem null
    for col in ["feedback_id", "type", "channel", "sentiment", "message", "created_at"]:
        nulls = df[col].isnull().sum()
        if nulls > 0:
            raise ValueError(f"Coluna obrigatória '{col}' tem {nulls} nulls inesperados!")

    # Nulls esperados (documentados)
    cid_nulls  = df["customer_id"].isnull().sum()
    nps_nulls  = df["nps_score"].isnull().sum()
    res_nulls  = df["resolved"].isnull().sum()
    logging.info(f"customer_id nulls: {cid_nulls} (esperado: ~8)")
    logging.info(f"nps_score nulls: {nps_nulls} (esperado: ~168 — só preenchido em channel='nps')")
    logging.info(f"resolved nulls: {res_nulls} (esperado: ~68)")

    # nps_score range quando preenchido
    nps_filled = df["nps_score"].dropna()
    if not nps_filled.empty:
        out_of_range = nps_filled[(nps_filled < 0) | (nps_filled > 10)]
        if not out_of_range.empty:
            logging.warning(f"nps_score fora do range 0–10: {len(out_of_range)} registros")

    # Domínios
    invalid_type = df[~df["type"].isin(VALID_TYPES)]["type"].unique()
    if len(invalid_type):
        logging.warning(f"type fora do domínio: {invalid_type}")

    invalid_ch = df[~df["channel"].isin(VALID_CHANNELS)]["channel"].unique()
    if len(invalid_ch):
        logging.warning(f"channel fora do domínio: {invalid_ch}")

    invalid_sent = df[~df["sentiment"].isin(VALID_SENTIMENTS)]["sentiment"].unique()
    if len(invalid_sent):
        logging.warning(f"sentiment fora do domínio: {invalid_sent}")

    # Consistência: nps_score preenchido apenas em channel='nps'
    nps_channel_rows  = df[df["channel"] == "nps"]["nps_score"].notnull().sum()
    nps_other_rows    = df[df["channel"] != "nps"]["nps_score"].notnull().sum()
    logging.info(f"nps_score preenchido em channel='nps': {nps_channel_rows} | em outros canais: {nps_other_rows}")

    logging.info("✅ bronze_feedbacks validada com sucesso.")


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="bronze_feedbacks",
    description="Ingestão de feedbacks.csv → camada Bronze (SQLite)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "feedbacks", "desafio-prevision"],
) as dag:

    t1 = PythonOperator(task_id="validate_source", python_callable=validate_source)
    t2 = PythonOperator(task_id="load_to_bronze",  python_callable=load_to_bronze)
    t3 = PythonOperator(task_id="validate_bronze", python_callable=validate_bronze)

    t1 >> t2 >> t3
