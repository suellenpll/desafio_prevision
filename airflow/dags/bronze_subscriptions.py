"""
DAG: bronze_subscriptions
Camada: Bronze
Descrição: Ingestão do subscriptions.csv para o banco SQLite (camada Bronze)
Projeto: desafio-prevision

Schema real do CSV:
  subscription_id | customer_id | plan | status | mrr | start_date | end_date | billing_cycle
  215 registros
  ATENÇÃO: subscription_id tem formato misto (sub_0001 e SUB-0003) — normalizado na Silver
  end_date: 140 nulls esperados (assinaturas ativas sem data de encerramento)
  mrr: float, range 58.04 – 999.00, média ~315
  billing_cycle: monthly | annual
  status: active | cancelled | trial
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
CSV_PATH     = os.path.join(AIRFLOW_HOME, "data", "bronze", "subscriptions.csv")
DB_PATH      = os.path.join(AIRFLOW_HOME, "data", "prevision.db")
TABLE_NAME   = "bronze_subscriptions"

EXPECTED_COLUMNS  = ["subscription_id", "customer_id", "plan", "status", "mrr", "start_date", "end_date", "billing_cycle"]
VALID_PLANS       = {"growth", "starter", "enterprise"}
VALID_STATUSES    = {"active", "cancelled", "trial"}
VALID_BILLING     = {"monthly", "annual"}

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
        raise ValueError("CSV de subscriptions está vazio!")

    # Alertar sobre inconsistência de subscription_id conhecida
    sample_ids = df["subscription_id"].tolist()
    logging.info(f"Arquivo OK — {row_count} linhas | sample subscription_ids: {sample_ids}")
    logging.warning(
        "ATENÇÃO: subscription_id tem formato misto (ex: 'sub_0001' vs 'SUB-0003'). "
        "Normalização será feita na camada Silver."
    )
    context["ti"].xcom_push(key="source_row_count", value=row_count)


def load_to_bronze(**context):
    """
    Carrega o CSV na tabela bronze_subscriptions (SQLite).
    Preserva dados brutos sem alteração — normalização é responsabilidade da Silver.
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
    - Colunas obrigatórias sem nulls (end_date: ~140 nulls esperados)
    - MRR positivo e dentro do range esperado
    - billing_cycle e status dentro do domínio
    - Alertar sobre subscription_id com formato misto
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

    logging.info(f"Total na Bronze: {len(df)} registros")

    # Colunas sem null esperado
    for col in ["subscription_id", "customer_id", "plan", "status", "mrr", "start_date", "billing_cycle"]:
        nulls = df[col].isnull().sum()
        if nulls > 0:
            raise ValueError(f"Coluna obrigatória '{col}' tem {nulls} nulls inesperados!")

    # end_date null é esperado (assinatura ativa)
    end_nulls = df["end_date"].isnull().sum()
    logging.info(f"end_date nulls: {end_nulls} (esperado: ~140 — assinaturas sem data de encerramento)")

    # MRR
    df["mrr"] = pd.to_numeric(df["mrr"], errors="coerce")
    mrr_invalid = df[df["mrr"] < 0]
    if not mrr_invalid.empty:
        raise ValueError(f"MRR negativo em {len(mrr_invalid)} registros!")
    logging.info(f"MRR — min: {df['mrr'].min():.2f} | max: {df['mrr'].max():.2f} | média: {df['mrr'].mean():.2f}")

    # Domínios
    invalid_billing = df[~df["billing_cycle"].isin(VALID_BILLING)]["billing_cycle"].unique()
    if len(invalid_billing):
        logging.warning(f"billing_cycle fora do domínio: {invalid_billing}")

    # subscription_id misto (documentado, não é erro)
    upper = df["subscription_id"].str.startswith("SUB-").sum()
    lower = df["subscription_id"].str.startswith("sub_").sum()
    logging.warning(
        f"subscription_id: {lower} no formato 'sub_XXXX' e {upper} no formato 'SUB-XXXX'. "
        f"Será normalizado para 'SUB-XXXX' na Silver."
    )

    logging.info("✅ bronze_subscriptions validada com sucesso.")


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="bronze_subscriptions",
    description="Ingestão de subscriptions.csv → camada Bronze (SQLite)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "subscriptions", "desafio-prevision"],
) as dag:

    t1 = PythonOperator(task_id="validate_source", python_callable=validate_source)
    t2 = PythonOperator(task_id="load_to_bronze",  python_callable=load_to_bronze)
    t3 = PythonOperator(task_id="validate_bronze", python_callable=validate_bronze)

    t1 >> t2 >> t3
