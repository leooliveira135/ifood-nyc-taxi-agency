# 🚕 iFood NYC Taxi Agency

A data engineering and analytics project focused on processing, transforming, and analyzing **New York City Taxi** data using modern data-stack tools such as **AWS Glue, Athena, Iceberg, Terraform, and Python**.

This repository demonstrates an end-to-end data platform approach — from ingestion and ETL to analytics and infrastructure provisioning — inspired by real-world delivery and mobility use cases.

---

## 📌 Overview

The **iFood NYC Taxi Agency** project simulates how a data team could ingest large-scale public mobility datasets and make them analytics-ready for business insights.

The project covers:

- Programmatic ingestion of NYC Taxi data
- ETL pipelines using AWS Glue
- Table formats with Apache Iceberg
- Querying with Amazon Athena
- Infrastructure provisioning with Terraform
- Exploratory data analysis using Jupyter notebooks

Although the name references *iFood*, this is a **technical case study** and learning project — not an official iFood system.

---

## ✨ Features

- 📥 **Data ingestion** from public APIs and datasets
- 🔄 **ETL pipelines** orchestrated with AWS Glue
- 🧊 **Apache Iceberg tables** for scalable analytics
- 🧠 **Analytical queries** using Amazon Athena
- 📊 **Exploratory analysis** via Jupyter notebooks
- ☁️ **Infrastructure as Code** using Terraform
- 🧰 Modular, reusable Python codebase

---

## 📁 Project Structure

```
ifood-nyc-taxi-agency/
├── analysis/                    # Jupyter notebooks with exploratory analysis
│   ├── average_passager_count.ipynb
│   └── average_total_amount.ipynb
│
├── src/                         # Application source code
│   ├── requirements.txt
│   └── ifood/
│       ├── main.py              # Project entry point
│       ├── vars.py              # Global configuration & constants
│       │
│       ├── api/                 # Data ingestion layer
│       │   └── fetch_data.py
│       │
│       ├── etl/                 # ETL & Glue jobs
│       │   ├── glue_setup.py
│       │   └── etl_process.py
│       │
│       └── aws/                 # AWS integrations
│           ├── credentials.py
│           ├── s3_bucket.py
│           ├── glue_catalog.py
│           ├── glue_iceberg_job.py
│           └── athena_queries.py
│
├── terraform/                   # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   ├── output.tf
│   └── terraform_admin_policy.txt
│
├── setup.sh                     # Local environment bootstrap
├── README.md
└── LICENSE
```

---

## 🚀 Getting Started

### Prerequisites

Make sure you have the following installed:

- **Python 3.9+**
- **pip**
- **AWS CLI** (configured credentials)
- **Terraform** (>= 1.3 recommended)

Optional but recommended:

- Docker
- Jupyter Notebook

---

### Installation

1. **Clone the repository**

```bash
git clone https://github.com/leooliveira135/ifood-nyc-taxi-agency.git
cd ifood-nyc-taxi-agency
```

2. **Run the setup script**

```bash
bash setup.sh
```

3. **Install Python dependencies**

```bash
pip install -r src/requirements.txt
```

---

## ▶️ Usage

### Run the ETL pipeline

```bash
python src/ifood/main.py
```

This will:

- Fetch NYC Taxi data
- Upload data to S3
- Create Glue catalogs
- Run Iceberg-based ETL jobs

### Run analytical queries

Athena queries are defined in:

```
src/ifood/aws/athena_queries.py
```

---

## 📊 Analysis

The `analysis/` folder contains Jupyter notebooks focused on business insights such as:

- Average passenger count per trip
- Average total amount by trip

These notebooks assume data is already processed and available in analytical tables.

---

## 🧪 Testing

This project does not yet include a full automated test suite.

Recommended next steps:

- Add unit tests for ETL logic
- Mock AWS services using `moto`
- Validate schema evolution for Iceberg tables

---

## 🛣 Roadmap

- [x] Initial project structure
- [x] Basic ETL pipeline
- [x] Athena analytics
- [ ] Add data quality checks
- [ ] Add orchestration (Airflow / Step Functions)
- [ ] CI/CD pipeline

---

## 🤝 Contributing

Contributions are welcome!

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Open a Pull Request

Please keep commits small and well-documented.

---

## 🛡 License

This project is licensed under the **GPL-3.0 License**.

See the [LICENSE](./LICENSE) file for details.

---

## 🙏 Acknowledgments

- NYC Taxi & Limousine Commission (public datasets)
- Apache Iceberg community
- AWS Glue & Athena documentation