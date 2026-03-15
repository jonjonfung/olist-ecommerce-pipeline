# 🛒 Olist E-Commerce Data Pipeline

## 🌐 Live Dashboard
👉 [View Live Dashboard](your_streamlit_url_here)

An end-to-end data engineering pipeline built on AWS using the Brazilian E-Commerce dataset from Kaggle.

## 🏗️ Architecture
```
Bronze Layer (Raw CSVs in S3)
    → AWS Glue PySpark ETL Job (Clean & Transform)
        → Silver Layer (Clean Parquet in S3)
            → Athena SQL (Aggregations)
                → Gold Layer (Aggregated Parquet in S3)
                    → Streamlit Dashboard
```

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Storage | AWS S3 |
| ETL | AWS Glue (PySpark) |
| Query Engine | AWS Athena |
| Data Format | Parquet + Snappy compression |
| Dashboard | Streamlit + Plotly |
| Architecture | Medallion (Bronze/Silver/Gold) |

## 📊 Dashboard

The Streamlit dashboard visualizes:
- Total orders, revenue, customers and sellers
- Monthly revenue trend
- Top 10 product categories by revenue
- Payment method breakdown
- Average delivery time by state

## 📁 Project Structure
```
├── glue/
│   └── transform.py        # PySpark ETL cleaning script
├── athena/
│   └── queries.sql         # SQL table definitions and transformations
├── dashboard/
│   └── dashboard.py        # Streamlit dashboard
└── README.md
```

## 🪣 S3 Structure
```
olist-ecommerce-pipeline-john/
  ├── bronze/     # Raw CSV files
  ├── silver/     # Cleaned Parquet files
  ├── gold/       # Aggregated Parquet files
  └── output/     # Athena query results
```

## 🚀 How to Run

### 1. Set up AWS
- Create an S3 bucket
- Upload raw CSVs to bronze/ folder
- Run the Glue ETL job from glue/transform.py
- Create Athena tables using athena/queries.sql

### 2. Run the dashboard
```bash
pip install streamlit boto3 plotly awswrangler
streamlit run dashboard/dashboard.py
```

## 📦 Dataset

[Brazilian E-Commerce Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

- 100k orders from 2016 to 2018
- 9 CSV files covering orders, customers, products, sellers and reviews
