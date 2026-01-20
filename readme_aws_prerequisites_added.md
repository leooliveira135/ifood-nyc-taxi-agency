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

## 🔐 AWS Account & IAM Prerequisites (Required)

> ⚠️ **Important**: This project assumes an AWS identity is **already configured and authenticated** before running any setup scripts or Terraform commands.

Before running `setup.sh`, `setup.py`, or any Terraform command, you **must** have a working AWS user or role with valid credentials.

### ✅ Required AWS Setup

You need **one AWS identity** (user or role) that:

- Can authenticate successfully with **AWS STS**
- Has permissions to create and manage:
  - IAM users and roles
  - S3 buckets and objects
  - AWS Glue resources (catalogs, crawlers, jobs)
  - Amazon Athena resources

This identity is referred to as the **bootstrap / admin identity**.

> 🔑 The bootstrap identity is used **only to provision infrastructure**.  
> It is **not** the same IAM user that runs Glue jobs or data pipelines.

---

### 🔍 Verify AWS Credentials (Mandatory Check)

Before proceeding, run:

```bash
aws sts get-caller-identity
```

You **must** see a valid AWS Account ID and ARN.

Example output:
```json
{
  "Account": "123456789012",
  "Arn": "arn:aws:iam::123456789012:user/admin",
  "UserId": "AIDA..."
}
```

If this command fails, **do not continue** — Terraform and the setup scripts will fail.

---

### 👤 IAM User Separation (Critical)

This project follows a **two-identity model**:

| Purpose | Identity |
|------|--------|
| Run Terraform & setup scripts | **Admin / bootstrap user or role** |
| Run Glue, Athena, ETL pipelines | `terraform-aws` (created by Terraform) |

🚫 **Do NOT** run Terraform using the same IAM user that Terraform creates (`terraform-aws`).  
Doing so will invalidate credentials during execution and result in STS errors such as:

- `InvalidClientTokenId`
- `NoSuchEntity`
- `AccessDenied`

---

### 🧰 AWS CLI Profile (Recommended)

Configure a dedicated admin profile:

```bash
aws configure --profile default
export AWS_PROFILE=default
```

Terraform will automatically use this profile.

---

### 🛑 Common Authentication Pitfalls

- ❌ Using expired temporary credentials (`AWS_SESSION_TOKEN`)
- ❌ Mixing AWS SSO credentials with static access keys
- ❌ Running Terraform as the same IAM user it manages
- ❌ Missing `sts:GetCallerIdentity` permission

If you encounter authentication-related errors, **fix AWS credentials first**, before debugging Terraform or Python code.

---

## 🚀 Getting Started

### Prerequisites

Make sure you have the following installed:

- **Python 3.9+**
- **pip**
- **AWS CLI** (authenticated – see section above)
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

---

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

