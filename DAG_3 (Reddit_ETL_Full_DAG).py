from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import praw
import pandas as pd
import boto3
import psycopg2

# ----- CONFIG -----
LOCAL_FILE = "/tmp/reddit_posts.csv"
S3_BUCKET = "reddit-etl-pipeline-data"
S3_KEY = "raw/reddit_posts.csv"
REDSHIFT_TABLE = "reddit_posts"
REDSHIFT_COPY_SQL = f"""
    COPY {REDSHIFT_TABLE}
    FROM 's3://{S3_BUCKET}/{S3_KEY}'
    IAM_ROLE 'arn:aws:iam::<your-account-id>:role/<your-redshift-iam-role>'
    FORMAT AS CSV
    IGNOREHEADER 1;
"""
# Make sure to replace <your-redshift-iam-role-arn> with your actual IAM role ARN in the above config block code

# ----- TASK 1: FETCH REDDIT DATA -----
def fetch_reddit_posts(**kwargs):
    config = Variable.get("reddit_config", deserialize_json=True)
    reddit = praw.Reddit(
        client_id=config["client_id"],
        client_secret=config["client_secret"],
        user_agent=config["user_agent"]
    )

    subreddit = reddit.subreddit("datascience")
    posts = []

    for post in subreddit.hot(limit=25):
        posts.append({
            "id": post.id,
            "title": post.title,
            "score": post.score,
            "url": post.url,
            "created_utc": datetime.utcfromtimestamp(post.created_utc).isoformat(),
            "num_comments": post.num_comments,
            "author": str(post.author),
            "subreddit": str(post.subreddit)
        })

    df = pd.DataFrame(posts)
    df.to_csv(LOCAL_FILE, index=False)
    print(f"Saved {len(df)} posts to {LOCAL_FILE}")

# ----- TASK 2: UPLOAD TO S3 -----
def upload_to_s3(**kwargs):
    creds = Variable.get("aws_credentials", deserialize_json=True)
    session = boto3.Session(
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        region_name=creds["region_name"]
    )
    s3 = session.client("s3")
    s3.upload_file(LOCAL_FILE, S3_BUCKET, S3_KEY)
    print(f"Uploaded {LOCAL_FILE} to s3://{S3_BUCKET}/{S3_KEY}")

# ----- TASK 3: LOAD TO REDSHIFT -----
def load_to_redshift(**kwargs):
    creds = Variable.get("redshift_credentials", deserialize_json=True)
    conn = psycopg2.connect(
        host=creds["host"],
        dbname=creds["database"],
        user=creds["user"],
        password=creds["password"],
        port=5439
    )
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {REDSHIFT_TABLE};")  # optional
    cursor.execute(REDSHIFT_COPY_SQL)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded data into Redshift table {REDSHIFT_TABLE}")

# ----- DAG DEFINITION -----
default_args = {
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}

with DAG(
    dag_id="reddit_etl_dag_full",
    default_args=default_args,
    schedule="@daily",
    description="Reddit → S3 → Redshift full ETL pipeline",
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_reddit_data",
        python_callable=fetch_reddit_posts
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    load_task = PythonOperator(
        task_id="load_to_redshift",
        python_callable=load_to_redshift
    )

    fetch_task >> upload_task >> load_task
