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
├── analysis/
├── src/
├── terraform/
├── setup.sh
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
  - AWS Glue resources
  - Amazon Athena resources

This identity is referred to as the **bootstrap / admin identity**, even though it uses the `default` AWS CLI profile.

---

### 🔍 Verify AWS Credentials (Mandatory Check)

```bash
aws sts get-caller-identity
```

This command **must succeed** before continuing.

---

### 👤 IAM User Separation (Critical)

| Purpose | Identity |
|------|--------|
| Run Terraform & setup scripts | **Bootstrap identity (default profile)** |
| Run Glue, Athena, ETL pipelines | `terraform-aws` (created by Terraform) |

🚫 **Do NOT** run Terraform using the same IAM user that Terraform creates.

---

### 🧰 AWS CLI Profile (Using `default`)

Configure AWS credentials using the default profile:

```bash
aws configure
export AWS_PROFILE=default
```

Terraform will automatically use this profile.

---

## 🚀 Getting Started

Follow the steps only after AWS authentication is verified.

---

## 🛡 License

GPL-3.0
