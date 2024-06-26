from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from plugins.mongodb_hook import MongoDBHook
from plugins.sentiment_analysis import SentimentAnalyzer
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sentiment_analysis_to_postgres',
    default_args=default_args,
    description='Get data from MongoDB, perform Sentiment Analysis, and load to Postgres',
    schedule_interval=timedelta(days=1),
)

def analyze_and_load():
    mongodb_hook = MongoDBHook()
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

    db = mongodb_hook.get_conn()['your_database']
    collection = db['your_collection']

    # Mengambil data dari MongoDB
    data = list(collection.find())

    # Melakukan analisis sentimen
    analyzer = SentimentAnalyzer()
    for item in data:
        sentiment = analyzer.analyze(item['headline'])
        item['sentiment'] = sentiment

    # Memuat hasil ke Postgres
    insert_sql = """
    INSERT INTO news_sentiment (id, headline, sentiment)
    VALUES (%s, %s, %s)
    """
    postgres_hook.insert_rows('news_sentiment', 
                              [(item['id'], item['headline'], item['sentiment']) for item in data])

task = PythonOperator(
    task_id='analyze_and_load',
    python_callable=analyze_and_load,
    dag=dag,
) 