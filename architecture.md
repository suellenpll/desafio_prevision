# Arquitetura e Modelo de Dados

## Arquitetura do Pipeline

```mermaid
flowchart LR
    subgraph Fontes
        C[customers.csv]
        S[subscriptions.csv]
        F[feedbacks.csv]
    end

    subgraph Bronze["🟡 Bronze — Ingestão bruta"]
        BC[bronze_customers]
        BS[bronze_subscriptions]
        BF[bronze_feedbacks]
    end

    subgraph Silver["🟢 Silver — Limpeza e enriquecimento"]
        SC[silver_customers]
        SS[silver_subscriptions]
        SF[silver_feedbacks]
    end

    subgraph Gold["🟣 Gold — KPIs analíticos"]
        GCS[gold_customer_subscription]
        GM[gold_mrr_by_dimension]
        GCA[gold_churn_analysis]
        GF[gold_feedback_metrics]
    end

    C --> BC --> SC
    S --> BS --> SS
    F --> BF --> SF

    SC --> GCS
    SS --> GCS
    GCS --> GM
    GCS --> GCA
    SF --> GF
    SC --> GF

    subgraph Infra["⚙️ Infraestrutura"]
        AF[Apache Airflow\nOrquestração de DAGs]
        DB[(SQLite\nprevision.db)]
    end

    Bronze --> DB
    Silver --> DB
    Gold --> DB
    AF -.-> Bronze
    AF -.-> Silver
    AF -.-> Gold
```

**Fluxo de execução das DAGs:**

```
bronze_customers ──┐
bronze_subscriptions ──┼──► silver_gold_transform
bronze_feedbacks ──┘

Cada DAG bronze executa:
  validate_source → load_to_bronze → validate_bronze
```

---

## Modelo de Dados

```mermaid
erDiagram
    silver_customers ||--o{ silver_subscriptions : "tem"
    silver_customers ||--o{ silver_feedbacks : "envia"
    silver_customers ||--o{ gold_customer_subscription : "integra"
    silver_subscriptions ||--o{ gold_customer_subscription : "integra"
    gold_customer_subscription ||--o{ gold_mrr_by_dimension : "agrega"
    gold_customer_subscription ||--o{ gold_churn_analysis : "analisa"
    silver_feedbacks ||--o{ gold_feedback_metrics : "agrega"

    silver_customers {
        string customer_id PK
        string name
        string email
        string plan
        string segment
        string status
        datetime created_at
        string country
        int customer_age_days
    }

    silver_subscriptions {
        string subscription_id PK
        string customer_id FK
        string plan
        string status
        float mrr
        datetime start_date
        datetime end_date
        string billing_cycle
        bool is_active
        int subscription_age_days
    }

    silver_feedbacks {
        string feedback_id PK
        string customer_id FK
        string type
        string channel
        string sentiment
        string message
        datetime created_at
        float nps_score
        bool resolved
    }

    gold_customer_subscription {
        string customer_id FK
        string subscription_id FK
        string plan
        string segment
        string customer_status
        string subscription_status
        float mrr
        bool is_active
        string country
    }

    gold_mrr_by_dimension {
        string dimension
        string dimension_value
        float total_mrr
        float active_mrr
        int subscription_count
        int active_subscription_count
        float avg_mrr
    }

    gold_churn_analysis {
        string dimension
        string dimension_value
        int total_customers
        int churned_customers
        float churn_rate_pct
        int cancelled_subscriptions
        float avg_mrr_lost
    }

    gold_feedback_metrics {
        string dimension
        string dimension_value
        int feedback_count
        float avg_nps_score
        float promoters_pct
        float detractors_pct
        float nps_calc
        float positive_pct
        float negative_pct
        float resolution_rate_pct
    }
```
