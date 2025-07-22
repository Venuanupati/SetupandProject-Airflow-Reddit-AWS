from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import praw
import pandas as pd
import json
import os

def fetch_reddit_posts(**kwargs):
    config = Variable.get("reddit_config", deserialize_json=True)
    reddit = praw.Reddit(
        client_id=config['client_id'],
        client_secret=config['client_secret'],
        user_agent=config['user_agent']
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
    output_path = "/tmp/reddit_posts.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} posts to {output_path}")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id="reddit_etl_dag",
    default_args=default_args,
    schedule="@daily",
    description="Fetch Reddit posts and store them",
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_reddit_data",
        python_callable=fetch_reddit_posts
    )
