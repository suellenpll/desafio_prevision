"""
DAG: silver_gold_transform
Camadas: Silver → Gold
Descrição: Limpeza/padronização (Silver) + Joins e KPIs analíticos (Gold)
Projeto: desafio-prevision

Dependências: bronze_customers, bronze_subscriptions, bronze_feedbacks devem rodar antes.

Fluxo de tasks:
  silver_customers ──┐
  silver_subscriptions ──┼──► gold_customer_subscription ──► gold_mrr_by_dimension
  silver_feedbacks ──┘    └──► gold_churn_analysis
                          └──► gold_feedback_metrics (+ silver_feedbacks)

Tabelas Gold geradas:
  - gold_customer_subscription   → visão desnormalizada cliente + assinatura
  - gold_mrr_by_dimension        → MRR total/ativo agrupado por plan, segment, billing_cycle
  - gold_churn_analysis          → análise de churn por plano e segmento
  - gold_feedback_metrics        → NPS e sentimento por channel, type, plan, segment
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
DB_PATH      = os.path.join(AIRFLOW_HOME, "data", "prevision.db")

default_args = {
    "owner": "desafio-prevision",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def read_table(conn, table: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {table}", conn)

def write_table(conn, df: pd.DataFrame, table: str):
    df.to_sql(table, conn, if_exists="replace", index=False)
    logging.info(f"'{table}' gravada: {len(df)} registros.")

# =============================================================================
# SILVER — CUSTOMERS
# =============================================================================

def silver_customers(**context):
    """
    Limpeza de customers:
    - Dedup por customer_id (mantém último)
    - strip + lower em plan, segment, status
    - created_at → datetime
    - country null → 'unknown'
    - Adiciona customer_age_days (dias desde created_at até hoje)
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = read_table(conn, "bronze_customers")

    logging.info(f"Bronze customers: {len(df)} registros")

    df = df.dropna(subset=["customer_id"])
    df = df.drop_duplicates(subset=["customer_id"], keep="last")

    for col in ["plan", "segment", "status"]:
        df[col] = df[col].astype(str).str.strip().str.lower()

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # country null → 'unknown'
    df["country"] = df["country"].fillna("unknown").str.strip().str.upper()

    # Idade do cliente em dias
    today = pd.Timestamp.utcnow().normalize()
    df["customer_age_days"] = (today - df["created_at"]).dt.days

    df["_processed_at"] = datetime.utcnow().isoformat()
    df = df[[c for c in df.columns if c != "_dag_run_id"]]

    with sqlite3.connect(DB_PATH) as conn:
        write_table(conn, df, "silver_customers")

    logging.info(f"✅ silver_customers: {len(df)} registros.")


# =============================================================================
# SILVER — SUBSCRIPTIONS
# =============================================================================

def silver_subscriptions(**context):
    """
    Limpeza de subscriptions:
    - Normaliza subscription_id: 'sub_0001' e 'SUB-0003' → 'SUB-XXXX' (uppercase, hífen)
    - Dedup por subscription_id normalizado (mantém último)
    - strip + lower em plan, status, billing_cycle
    - start_date / end_date → datetime
    - mrr: numérico, remove negativos
    - is_active: True se status='active' ou end_date é null/futura
    - subscription_age_days: dias desde start_date
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = read_table(conn, "bronze_subscriptions")

    logging.info(f"Bronze subscriptions: {len(df)} registros")

    df = df.dropna(subset=["customer_id"])

    # Normalizar subscription_id: sub_0001 → SUB-0001 | SUB-0003 → SUB-0003
    def normalize_sub_id(sid):
        if pd.isna(sid):
            return sid
        sid = str(sid).strip().upper()          # SUB_0001 ou SUB-0003
        sid = sid.replace("SUB_", "SUB-")       # garante hífen
        # Caso venha como 'SUB0001' sem separador
        if sid.startswith("SUB") and len(sid) > 3 and sid[3].isdigit():
            sid = "SUB-" + sid[3:]
        return sid

    df["subscription_id"] = df["subscription_id"].apply(normalize_sub_id)
    df = df.drop_duplicates(subset=["subscription_id"], keep="last")

    for col in ["plan", "status", "billing_cycle"]:
        df[col] = df[col].astype(str).str.strip().str.lower()

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce")

    df["mrr"] = pd.to_numeric(df["mrr"], errors="coerce")
    df = df[df["mrr"].notna() & (df["mrr"] >= 0)]

    today = pd.Timestamp.utcnow().normalize()
    df["is_active"] = (df["status"] == "active") & (df["end_date"].isna() | (df["end_date"] > today))
    df["subscription_age_days"] = (today - df["start_date"]).dt.days

    df["_processed_at"] = datetime.utcnow().isoformat()
    df = df[[c for c in df.columns if c != "_dag_run_id"]]

    with sqlite3.connect(DB_PATH) as conn:
        write_table(conn, df, "silver_subscriptions")

    active_count = df["is_active"].sum()
    logging.info(f"✅ silver_subscriptions: {len(df)} registros | {active_count} ativos.")


# =============================================================================
# SILVER — FEEDBACKS
# =============================================================================

def silver_feedbacks(**context):
    """
    Limpeza de feedbacks:
    - Remove registros sem feedback_id
    - Dedup por feedback_id (mantém último)
    - Remove 8 registros sem customer_id (não rastreáveis)
    - strip + lower em type, channel, sentiment
    - created_at → datetime
    - nps_score: numérico, mantém null onde channel != 'nps' (é correto)
      Valida range 0-10 onde preenchido
    - resolved: converte para boolean, null → False (feedback sem resolução = não resolvido)
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = read_table(conn, "bronze_feedbacks")

    logging.info(f"Bronze feedbacks: {len(df)} registros")

    df = df.dropna(subset=["feedback_id"])
    df = df.drop_duplicates(subset=["feedback_id"], keep="last")

    # Remove feedbacks sem customer_id (não rastreáveis)
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    logging.info(f"Removidos {before - len(df)} feedbacks sem customer_id")

    for col in ["type", "channel", "sentiment"]:
        df[col] = df[col].astype(str).str.strip().str.lower()

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # nps_score: apenas válido em channel='nps', range 0-10
    df["nps_score"] = pd.to_numeric(df["nps_score"], errors="coerce")
    nps_mask = df["channel"] == "nps"
    invalid_nps = df[nps_mask & df["nps_score"].notna() & ~df["nps_score"].between(0, 10)]
    if not invalid_nps.empty:
        logging.warning(f"nps_score fora de range em {len(invalid_nps)} registros — definido como null")
        df.loc[invalid_nps.index, "nps_score"] = None

    # resolved: boolean, null → False
    df["resolved"] = df["resolved"].map(
        lambda x: True if str(x).strip().lower() in ("true", "1", "yes") else False
    )

    df["_processed_at"] = datetime.utcnow().isoformat()
    df = df[[c for c in df.columns if c != "_dag_run_id"]]

    with sqlite3.connect(DB_PATH) as conn:
        write_table(conn, df, "silver_feedbacks")

    logging.info(f"✅ silver_feedbacks: {len(df)} registros.")


# =============================================================================
# GOLD — CUSTOMER SUBSCRIPTION (JOIN PRINCIPAL)
# =============================================================================

def gold_customer_subscription(**context):
    """
    Join LEFT entre silver_subscriptions e silver_customers.
    Tabela desnormalizada com todos os atributos por assinatura.
    Campos herdados: plan, segment, status_customer, status_subscription,
                     mrr, billing_cycle, is_active, start_date, end_date,
                     country, customer_age_days, subscription_age_days
    """
    with sqlite3.connect(DB_PATH) as conn:
        customers     = read_table(conn, "silver_customers")
        subscriptions = read_table(conn, "silver_subscriptions")

    # Join: assinatura é a granularidade principal
    df = subscriptions.merge(
        customers[["customer_id", "name", "email", "segment", "status",
                   "country", "customer_age_days", "created_at"]],
        on="customer_id",
        how="left",
        suffixes=("_sub", "_cust")
    )

    # Renomear status para evitar ambiguidade
    if "status_sub" in df.columns:
        df.rename(columns={"status_sub": "subscription_status", "status_cust": "customer_status"}, inplace=True)

    df["_gold_generated_at"] = datetime.utcnow().isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        write_table(conn, df, "gold_customer_subscription")

    logging.info(f"✅ gold_customer_subscription: {len(df)} registros.")


# =============================================================================
# GOLD — MRR POR DIMENSÃO
# =============================================================================

def gold_mrr_by_dimension(**context):
    """
    KPIs de MRR agrupados por dimensões relevantes.
    Métricas por grupo:
      - total_mrr: soma do MRR
      - active_mrr: MRR apenas de assinaturas ativas
      - subscription_count: total de assinaturas
      - active_subscription_count: total de ativas
      - avg_mrr: MRR médio por assinatura
    Dimensões: plan | segment | billing_cycle | country
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = read_table(conn, "gold_customer_subscription")

    def aggregate_mrr(group_col):
        agg = df.groupby(group_col, dropna=False).apply(
            lambda g: pd.Series({
                "total_mrr":                round(g["mrr"].sum(), 2),
                "active_mrr":               round(g.loc[g["is_active"] == True, "mrr"].sum(), 2),
                "subscription_count":       len(g),
                "active_subscription_count": int((g["is_active"] == True).sum()),
                "avg_mrr":                  round(g["mrr"].mean(), 2),
            })
        ).reset_index()
        agg.insert(0, "dimension", group_col)
        agg.rename(columns={group_col: "dimension_value"}, inplace=True)
        return agg

    results = []
    for dim in ["plan", "segment", "billing_cycle", "country"]:
        if dim in df.columns:
            results.append(aggregate_mrr(dim))

    metrics = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    metrics["_gold_generated_at"] = datetime.utcnow().isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        write_table(conn, metrics, "gold_mrr_by_dimension")

    logging.info(f"✅ gold_mrr_by_dimension: {len(metrics)} linhas de métricas.")


# =============================================================================
# GOLD — CHURN ANALYSIS
# =============================================================================

def gold_churn_analysis(**context):
    """
    Análise de churn por plano e segmento.
    Métricas:
      - total_customers: clientes únicos
      - churned_customers: clientes com status='churned'
      - churn_rate_pct: % de churn
      - cancelled_subscriptions: assinaturas canceladas
      - avg_mrr_lost: MRR médio das assinaturas canceladas (receita em risco)
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = read_table(conn, "gold_customer_subscription")

    results = []
    for dim in ["plan", "segment"]:
        if dim not in df.columns:
            continue
        agg = df.groupby(dim, dropna=False).apply(
            lambda g: pd.Series({
                "total_customers":          g["customer_id"].nunique(),
                "churned_customers":        (g["customer_status"] == "churned").sum()
                                            if "customer_status" in g.columns else 0,
                "churn_rate_pct":           round(
                    (g["customer_status"] == "churned").sum() / g["customer_id"].nunique() * 100, 2
                ) if g["customer_id"].nunique() > 0 else 0,
                "cancelled_subscriptions":  (g["subscription_status"] == "cancelled").sum()
                                            if "subscription_status" in g.columns else 0,
                "avg_mrr_lost":             round(
                    g.loc[g.get("subscription_status", pd.Series()) == "cancelled", "mrr"].mean(), 2
                ) if "subscription_status" in g.columns else 0,
            })
        ).reset_index()
        agg.insert(0, "dimension", dim)
        agg.rename(columns={dim: "dimension_value"}, inplace=True)
        results.append(agg)

    churn = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    churn["_gold_generated_at"] = datetime.utcnow().isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        write_table(conn, churn, "gold_churn_analysis")

    logging.info(f"✅ gold_churn_analysis: {len(churn)} linhas.")


# =============================================================================
# GOLD — FEEDBACK METRICS (NPS + SENTIMENTO)
# =============================================================================

def gold_feedback_metrics(**context):
    """
    Métricas de feedback enriquecidas com dados do cliente.
    KPIs por dimensão (channel, type, plan, segment):
      - feedback_count: volume total
      - avg_nps_score: média NPS (apenas onde preenchido, channel='nps')
      - promoters_pct: % nps_score >= 9
      - detractors_pct: % nps_score <= 6
      - nps_calc: promoters% - detractors%
      - positive_pct: % sentiment='positive'
      - negative_pct: % sentiment='negative'
      - resolution_rate_pct: % resolved=True
    """
    with sqlite3.connect(DB_PATH) as conn:
        feedbacks = read_table(conn, "silver_feedbacks")
        customers = read_table(conn, "silver_customers")

    df = feedbacks.merge(
        customers[["customer_id", "plan", "segment"]],
        on="customer_id", how="left"
    )

    def feedback_kpis(g):
        total    = len(g)
        nps_rows = g["nps_score"].dropna()

        promoters  = (nps_rows >= 9).sum()
        detractors = (nps_rows <= 6).sum()
        nps_total  = len(nps_rows)

        return pd.Series({
            "feedback_count":      total,
            "avg_nps_score":       round(nps_rows.mean(), 2) if nps_total > 0 else None,
            "promoters_pct":       round(promoters / nps_total * 100, 2) if nps_total > 0 else None,
            "detractors_pct":      round(detractors / nps_total * 100, 2) if nps_total > 0 else None,
            "nps_calc":            round((promoters - detractors) / nps_total * 100, 2) if nps_total > 0 else None,
            "positive_pct":        round((g["sentiment"] == "positive").sum() / total * 100, 2),
            "negative_pct":        round((g["sentiment"] == "negative").sum() / total * 100, 2),
            "resolution_rate_pct": round(g["resolved"].sum() / total * 100, 2),
        })

    results = []
    for dim in ["channel", "type", "plan", "segment"]:
        if dim not in df.columns:
            continue
        agg = df.groupby(dim, dropna=False).apply(feedback_kpis).reset_index()
        agg.insert(0, "dimension", dim)
        agg.rename(columns={dim: "dimension_value"}, inplace=True)
        results.append(agg)

    metrics = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    metrics["_gold_generated_at"] = datetime.utcnow().isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        write_table(conn, metrics, "gold_feedback_metrics")

    logging.info(f"✅ gold_feedback_metrics: {len(metrics)} linhas.")


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="silver_gold_transform",
    description="Silver (limpeza) + Gold (joins e KPIs) — desafio-prevision",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["silver", "gold", "transform", "desafio-prevision"],
) as dag:

    # ── Silver ────────────────────────────────────────────────────────────────
    t_silver_customers     = PythonOperator(task_id="silver_customers",     python_callable=silver_customers)
    t_silver_subscriptions = PythonOperator(task_id="silver_subscriptions", python_callable=silver_subscriptions)
    t_silver_feedbacks     = PythonOperator(task_id="silver_feedbacks",     python_callable=silver_feedbacks)

    # ── Gold join principal ───────────────────────────────────────────────────
    t_gold_cs = PythonOperator(task_id="gold_customer_subscription", python_callable=gold_customer_subscription)

    # ── Gold métricas ─────────────────────────────────────────────────────────
    t_gold_mrr     = PythonOperator(task_id="gold_mrr_by_dimension",  python_callable=gold_mrr_by_dimension)
    t_gold_churn   = PythonOperator(task_id="gold_churn_analysis",    python_callable=gold_churn_analysis)
    t_gold_fb      = PythonOperator(task_id="gold_feedback_metrics",  python_callable=gold_feedback_metrics)

    # ── Dependências ──────────────────────────────────────────────────────────
    #
    #  silver_customers ──┐
    #                     ├──► gold_customer_subscription ──► gold_mrr_by_dimension
    #  silver_subscriptions ─┘                            └──► gold_churn_analysis
    #
    #  silver_feedbacks ──────────────────────────────────────► gold_feedback_metrics
    #  (gold_feedback_metrics também usa silver_customers via join interno)

    [t_silver_customers, t_silver_subscriptions] >> t_gold_cs
    t_gold_cs >> [t_gold_mrr, t_gold_churn]
    [t_silver_feedbacks, t_silver_customers] >> t_gold_fb
