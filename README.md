# Reddit ETL Pipeline using Airflow and AWS

This project implements an end-to-end ETL pipeline that extracts Reddit data using Apache Airflow, processes it, and loads it into AWS Redshift for warehousing and analysis.

### 🚀 Tech Stack

- **Apache Airflow** (on Google Cloud VM)
- **Reddit API** (via `praw`)
- **AWS S3** – raw data storage
- **AWS Glue** – data cataloging
- **AWS Athena** – SQL querying
- **AWS Redshift** – data warehouse

### 📦 Pipeline Overview

1. **Extract** Reddit posts using Reddit API  
2. **Transform** data into structured CSV format  
3. **Load** to S3 and catalog using AWS Glue  
4. **Query** data in Athena for validation  
5. **Ingest** into Amazon Redshift for analytics  

### 🗂️ Files Included

- `dag1.py`: Extract Reddit data and store locally  
- `dag2.py`: Upload CSV to S3  
- `dag3.py`: Full pipeline – Reddit → S3 → Redshift  
- `Reddit_ETL_Step_by_Step.pdf`: Complete setup and execution guide (already committed)


### 🛠️ How to Run

1. Clone the repo and follow the instructions in the PDF
2. Configure Airflow variables for Reddit, AWS, and Redshift credentials
3. Place the DAGs into your Airflow `dags/` directory
4. Trigger the DAGs on the Airflow


### 📊 Output

Reddit post data is now queryable in Amazon Redshift and ready for BI or analytics tools.

