# Desafio Prevision — Pipeline de Dados

Solução de dados que integra três fontes fornecidas, trata inconsistências, modela os dados de forma analítica e responde a perguntas de negócio concretas.

---

## Decisões Técnicas

### Linguagem
**Python 3.12** — escolhido pela maturidade do ecossistema de dados, compatibilidade com o Airflow e familiaridade com as bibliotecas de manipulação e análise.

### Armazenamento
**SQLite** — banco de dados relacional leve, sem necessidade de servidor, ideal para o volume de dados do desafio (~200–230 registros por fonte). O arquivo `prevision.db` concentra todas as camadas (Bronze, Silver e Gold) em um único lugar, facilitando consultas e portabilidade.

### Pipeline
**Apache Airflow 2.10.4 (standalone)** — orquestrador de DAGs que permite definir dependências entre tarefas, reprocessamento idempotente e rastreabilidade de execuções. O modo standalone simplifica a configuração sem necessidade de infraestrutura adicional. O pipeline segue a arquitetura medallion em três camadas:

- **Bronze** — ingestão dos CSVs brutos sem transformação
- **Silver** — limpeza, padronização e enriquecimento
- **Gold** — joins e KPIs analíticos prontos para consumo

### Transformação e Análise
**pandas** — utilizado nas DAGs para transformação dos dados em todas as camadas. A análise é apresentada diretamente via queries SQL sobre o banco SQLite e documentada neste README, sem necessidade de notebook ou dashboard externo para o escopo do desafio.

### Por que não dbt, Spark ou DuckDB?
Para o volume de dados apresentado (~600 registros no total), ferramentas como Spark introduziriam complexidade desnecessária. O dbt seria uma alternativa válida à camada Silver/Gold, mas o Airflow já cobre o requisito de orquestração e rastreabilidade. O SQLite atende plenamente ao requisito de armazenamento relacional sem overhead de configuração.

---

## Sumário

- [Tecnologias](#tecnologias)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Como Executar](#como-executar)
- [Qualidade de Dados](#qualidade-de-dados)
- [Modelagem Analítica](#modelagem-analítica)
- [Análise e Insights](#análise-e-insights)

---

## Tecnologias

- Python 3.12
- Apache Airflow 2.10.4 (standalone)
- SQLite
- pandas
- SQLAlchemy

---

## Estrutura do Projeto

```
desafio_prevision/
├── airflow/
│   ├── dags/
│   │   ├── bronze_customers.py
│   │   ├── bronze_subscriptions.py
│   │   ├── bronze_feedbacks.py
│   │   └── silver_gold_transform.py
│   └── data/
│       ├── bronze/
│       │   ├── customers.csv
│       │   ├── subscriptions.csv
│       │   └── feedbacks.csv
│       └── prevision.db
└── README.md
```

---

## Como Executar

### Pré-requisitos

- Python 3.12
- WSL2 (Ubuntu) — o Airflow não tem suporte nativo no Windows

### Configuração do ambiente

```bash
# Criar e ativar o ambiente virtual
python3.12 -m venv venv
source venv/bin/activate

# Instalar dependências
pip install -r requirements.txt
```

### Inicializar o Airflow

```bash
export AIRFLOW_HOME=~/desafio_prevision/airflow
airflow db init
airflow standalone
```

Acesse `http://localhost:8080` com as credenciais geradas no terminal.

### Executar o pipeline

Execute as DAGs na seguinte ordem na interface do Airflow:

1. `bronze_customers`
2. `bronze_subscriptions`
3. `bronze_feedbacks`
4. `silver_gold_transform` (após as três bronze finalizarem)

---

## Qualidade de Dados

### Arquivos de entrada

| Arquivo | Registros | Colunas |
|---|---|---|
| customers.csv | 200 | 8 |
| subscriptions.csv | 215 | 8 |
| feedbacks.csv | 229 | 9 |

### Problemas encontrados e tratativas

#### customers.csv

| Problema | Ocorrências | Tratativa |
|---|---|---|
| `country` nulo | 8 registros | Preenchido com `"unknown"` |
| Sem duplicatas | — | Nenhuma ação necessária |

`country` nulo não invalida o cliente — apenas indica ausência de informação geográfica. Substituir por `"unknown"` preserva o registro e permite análise segmentada.

#### subscriptions.csv

| Problema | Ocorrências | Tratativa |
|---|---|---|
| `subscription_id` com formato misto (`sub_0001` vs `SUB-0003`) | Parte dos 215 | Normalizado para `SUB-XXXX` na Silver |
| `end_date` nulo | ~140 registros | Esperado — assinaturas ativas não têm data de encerramento |

O formato misto de `subscription_id` indica dois sistemas de cadastro distintos na origem. A normalização na Silver garante chaves únicas sem perda de dados.

#### feedbacks.csv

| Problema | Ocorrências | Tratativa |
|---|---|---|
| `customer_id` nulo | 8 registros | Removidos na Silver — não rastreáveis |
| `customer_id` com formato inválido (`CUST_XXXX`) | 5 registros | Normalizados: `"_"` → `"-"` |
| `nps_score` nulo fora do canal `nps` | ~168 registros | Esperado — NPS só é coletado no canal `nps` |
| `resolved` nulo | ~68 registros | Convertido para `False` |
| Datas futuras em `created_at` | ~15 registros | Mantidos com alerta — possível fuso horário |

Os 8 feedbacks sem `customer_id` são descartados pois não é possível cruzá-los com dados de negócio. Os IDs com formato `CUST_XXXX` são erros de digitação e corrigidos para manter a integridade referencial.

### Camadas do pipeline

```
Bronze  → dados brutos preservados, sem alteração
Silver  → limpeza, padronização e enriquecimento
Gold    → joins e KPIs analíticos prontos para consumo
```

---

## Modelagem Analítica

### Tabelas Gold geradas

| Tabela | Descrição |
|---|---|
| `gold_customer_subscription` | Visão desnormalizada cliente + assinatura |
| `gold_mrr_by_dimension` | MRR agrupado por plan, segment, billing_cycle e country |
| `gold_churn_analysis` | Taxa de churn e MRR em risco por plano e segmento |
| `gold_feedback_metrics` | NPS, sentimento e resolução por channel, type, plan e segment |

### Diagrama de relacionamento

```
silver_customers ──────────────────────────────────────────┐
       │                                                    │
       ├──── gold_customer_subscription ◄── silver_subscriptions
       │              │
       │              ├──► gold_mrr_by_dimension
       │              └──► gold_churn_analysis
       │
       └──── gold_feedback_metrics ◄── silver_feedbacks
```

O `customer_id` é a chave de ligação entre todas as tabelas, permitindo cruzar feedbacks com métricas de negócio diretamente.

---

## Análise e Insights

### Pergunta obrigatória: existe relação entre tipo de feedback e churn?

**Sim.** Clientes churned concentram uma proporção significativamente maior de feedbacks do tipo `complaint` em relação a clientes ativos. O tipo `praise` é praticamente ausente entre clientes que cancelaram.

#### Distribuição por tipo de feedback e status

| Tipo | Clientes Ativos | Clientes Churned |
|---|---|---|
| complaint | médio | **dominante** |
| bug | alto | significativo |
| feature_request | médio | neutro |
| support | médio | menor |
| praise | alto | **muito baixo** |

#### Variação por segmento de plano

| Plano | Segmento | Padrão |
|---|---|---|
| **starter** | SMB | Maior volume de complaints + maior taxa de churn. Clientes sensíveis a preço e suporte. |
| **growth** | Mid-Market | Complaints concentrados em bugs e tempo de resposta. Churn moderado. |
| **enterprise** | Enterprise | Menor volume relativo de negativos, mas com maior MRR em risco por cancelamento. |

### Insights adicionais

**MRR em risco:** clientes churned no plano `enterprise` representam perda desproporcional de receita — poucos em número, mas com os maiores valores de MRR por assinatura.

**NPS:** scores baixos (≤ 6, detratores) estão associados majoritariamente a feedbacks de `complaint` e `bug`. A ausência de NPS em canais não-nps limita a visão completa de satisfação.

**Taxa de resolução:** feedbacks de `bug` e `complaint` têm taxas de resolução abaixo da média, o que pode amplificar a insatisfação e acelerar o churn — especialmente no segmento SMB.

**Clientes em trial:** aqueles com feedbacks de `complaint` ou `bug` têm alto risco de não converter. Ações proativas de onboarding podem aumentar a taxa de conversão.

### Recomendações

1. Monitorar complaints em tempo real por segmento — criar alertas quando a proporção ultrapassar um threshold por plano.
2. Priorizar resolução de bugs para clientes Mid-Market — impacto direto no churn de um segmento com MRR relevante.
3. Rever proposta de valor para SMB/starter — o feedback recorrente de "preço alto para o que entrega" indica desalinhamento entre expectativa e entrega.
4. Expandir coleta de NPS para todos os canais — a lacuna atual impede visão completa da satisfação.
5. Criar um "health score" por cliente combinando tipo de feedback recente, sentimento, NPS e tempo sem interação positiva para antecipar churn.
