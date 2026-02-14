# Real-Time Payments Streaming Platform

A portfolio-ready **fintech data engineering project** that simulates real-time payment transactions, processes them with a streaming engine, flags suspicious activity, and exposes live alerts through a dashboard and database.

This project is designed to showcase **data engineering, streaming, and cloud skills** for internships at banks, fintechs, and data-heavy companies.

---

## 📌 High-Level Architecture

**Data Flow:**

1. **Producer** generates synthetic payment events and publishes them to a `transactions` topic.
2. **Streaming Processor** consumes events, applies transformations and fraud/velocity rules, and writes:
   - Clean events to a `transactions_clean` topic or DB.
   - Suspicious events to an `alerts` topic.
3. **Storage Layer** persists alerts and transactions (e.g., PostgreSQL).
4. **Dashboard** displays live alerts and simple analytics (Streamlit or similar).

---

## 🧱 Tech Stack

- **Languages:** Python, SQL
- **Streaming & Processing:**
  - Apache Kafka (event streaming)
  - Spark Structured Streaming (real-time processing)
- **Data Storage:**
  - PostgreSQL (transactions + alerts)
- **Visualization:**
  - Streamlit (live monitoring dashboard)
- **Infrastructure:**
  - Docker & Docker Compose for local orchestration
  - Azure for cloud deployment (Container Apps / VM / Database)

---

## 🚀 Features (MVP Scope)

- Synthetic **payment transaction generator**:
  - Fields such as `transaction_id`, `user_id`, `amount`, `currency`, `merchant_id`, `timestamp`, `location`.
- Real-time streaming pipeline:
  - Read from Kafka `transactions`.
  - Parse JSON into a typed schema.
  - Windowed aggregations (e.g., per-user or per-merchant spend per minute).
- Basic fraud/velocity rules, for example:
  - Single transaction above a threshold (e.g., 5,000).
  - More than N transactions per user in a short time window.
- Alerts:
  - Suspicious events written to an `alerts` topic and stored in Postgres.
- Dashboard:
  - Live table of recent alerts.
  - Simple charts for transaction volume and number of alerts over time.

---

## 📦 Project Structure (Suggested)

```text
.
├── docker-compose.yml
├── README.md
├── producer/
│   └── producer.py
├── streaming/
│   └── processor.py
├── consumers/
│   └── db_sink.py
├── dashboard/
│   └── app.py
└── config/
    ├── kafka_topics.yaml
    └── db_config.yaml
