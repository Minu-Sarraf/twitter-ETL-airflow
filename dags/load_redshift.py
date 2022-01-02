#!/usr/bin/env python



import time
import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
import json
import requests
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator



# attempt the connection to postgres
try:
    dbconnect = pg.connect(
    database="dev",
    user="postgres",
    password="123",
    host="127.0.0.1:5432"
   )
except Exception as error:
    print(error)

args = {
    'owner': 'test_owner',
    'start_date': datetime.datetime(2021,12,31,00,00,00),
    'end_date': datetime.datetime(2021, 12, 31, 5, 00, 00),
    'depends_on_past': False,
    'provide_context': True,
    'backfill': True
}

dag = DAG(
    dag_id='load_dimensions',
    default_args=args,
    schedule_interval='@hourly',
    catchup=True,
    max_active_runs=16
)

def load_tweet_data_py(**op_kwargs):
    print("Loading Tweet Data")

def extract(**op_kwargs):

    ts = op_kwargs['ts_var']

    print(f"Hello, Current Time:{time.strftime('%H:%M:%S')} Execution time is {ts}")


    print(ts)
    start = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S%z')
    # print(start)
    end = start + datetime.timedelta(minutes=10)
    start_time = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = end.strftime('%Y-%m-%dT%H:%M:%SZ')
    print("start_time", start_time)
    print("end_time", end_time)

    # credentials for twitter api, it needs to be kept in config file
    CONSUMER_KEY='EEZ6Nxv7DGs6cRlNB1ZNIkFVV'
    CONSUMER_SECRET='zMeBOqmIjba0XIADiTCQq6KiycI9RnPTIo3nXS4XYa5MIPVlQb'
    BEARER_TOKEN='AAAAAAAAAAAAAAAAAAAAANPsXQEAAAAAviI%2FyqiM1YTDZW5cAmp%2B6uRxCHw%3Dzlsm8DhMgAnjttXCBVXeEuDhu0eTSS8teyPOs0XqpwKhdox0MG'
    headers = {"Authorization": "Bearer {}".format(BEARER_TOKEN)}
    search_url="https://api.twitter.com/2/tweets/search/recent"
    query="covid"
    tweet_fields = "tweet.fields=text,created_at,referenced_tweets"
    user_fields = "user.fields=username,name,verified,location"
    
    url = "{}?query={}&{}&{}&expansions=author_id,referenced_tweets.id&max_results=10&start_time={}&end_time={}".format(
                search_url, query, tweet_fields, user_fields, start_time, end_time
            )

    response = requests.request("GET", url, headers=headers)
    print (response.json())

    # write to temp storage
    filename = 'response_{}.json'.format(op_kwargs['tsnodash_var'])
    with open('plugins/extraction/{}'.format(filename), 'w') as outfile:
        json.dump(response.json(), outfile)

#transform the raw tweet data into a meaningful dataframe    
def transform(**op_kwargs):
    print("Transform")

    # read json
    filename = 'response_{}.json'.format(op_kwargs['tsnodash_var'])
    tweet = json.load(open('plugins/extraction/{}'.format(filename)))

    # json to dataframe

    tweets = pd.DataFrame(tweet['data'])
    user_info = pd.DataFrame(tweet['includes']['users'])
    orignal_tweets = pd.DataFrame(tweet['includes']['tweets']).rename(columns={
    'text': 'original_text','id':'original_id'})
    tweets['referenced_tweets'] = tweets['referenced_tweets'].fillna(0)
    tweets['referenced_tweets_id'] = tweets.apply(lambda x: x['referenced_tweets'][0]['id'] if type(x['referenced_tweets']) == list else 0, 1)
    merged_db= tweets.merge(orignal_tweets[['original_id', 'original_text']], how='left', left_on='referenced_tweets_id', right_on='original_id')
    merged_db['original_text'] = merged_db['original_text'].fillna(merged_db['text'])
    data = merged_db.drop(['text', 'original_id', 'referenced_tweets', 'referenced_tweets_id'], 1)
    data = data.merge(user_info[['id', 'location']], how='left', left_on='author_id', right_on='id', suffixes=['','_y']).drop('id_y', 1)
    data['location'] = data['location'].fillna('')

    # write the pandas dataframe to plugins/transform
    filename = 'data_{}.csv'.format(op_kwargs['tsnodash_var'])
    data.to_csv('plugins/transform/{}'.format(filename))


def load_postgress(**op_kwargs):
   # read data
   filename = 'data_{}.csv'.format(op_kwargs['tsnodash_var'])
   data.read_csv('plugins/transform/{}'.format(filename))

   

def load_customer_dim(**op_kwargs):
    print(op_kwargs)
    ts = op_kwargs['ts_var']

    print(f"Hello, Current Time:{time.strftime('%H:%M:%S')} Execution time is {ts}")

    customer_df = pd.read_csv('plugins/data.csv')
    print(customer_df)

with dag:

    tweet_extraction = PythonOperator(
        task_id='tweet_extraction',
        python_callable=extract,
        provide_context=True,
        op_kwargs={
            'key':'value',
            'ts_var': '{{ ts }}',
            'tsnodash_var': '{{ ts_nodash }}'
        }
    )

    tweet_transform = PythonOperator(
        task_id='tweet_transform',
        python_callable=transform,
        provide_context=True,
        op_kwargs={
            'key':'value',
            'ts_var': '{{ ts }}',
            'tsnodash_var': '{{ ts_nodash }}'
        }
    )

    dummy_op = PythonOperator(
        task_id='tweet_pipeline',
        python_callable=load_dimension_py,
        op_kwargs={
            'key': 'value'
        }
    )

    end_operator = DummyOperator(task_id='stop_execution')

    dummy_op >> tweet_extraction >> tweet_transform >> end_operator

