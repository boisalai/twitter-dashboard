import pathlib
import pandas as pd
import asyncio
import tweetnlp
import re 
import html

from config import *
from pathlib import Path

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(name="Read Configuration File", log_prints=True)
def read_config() -> dict:
    config = get_config()
    return config

@task(name="Get GCS Path", retries=3, log_prints=True)
def get_gcs_path(config: dict) -> Path:
    """Download data from GCS"""
    bucket_filename = config['bucket_filename']
    gcs_bucket_block_name = config['gcs_bucket_block_name']
    gcs_path = f"data/{bucket_filename}.parquet"
    gcs_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(name="Read file from GCS", log_prints=True)
def read_from_gcs(path: Path) -> pd.DataFrame:
    """Read file from GCS"""
    df = pd.read_parquet(path)
    return df

@task(name="Clean Tweets", log_prints=True)
def clean_tweets(df: pd.DataFrame) -> pd.DataFrame:
    # Parse the tweets and removes punctuation, stop words, digits, and links.
    punctuations = """!()-![]{};:+'"\,<>./?@#$%^&*_~Â""" 
    for i in range(len(df)):
        tweet = df.loc[i, "content"]
        tweet = html.unescape(tweet) # Removes leftover HTML elements, such as &amp;.
        tweet = re.sub(r"@\w+", ' ', tweet) # Completely removes @'s, as other peoples' usernames mean nothing.
        tweet = re.sub(r'http\S+', ' ', tweet) # Removes links, as links provide no data in tweet analysis in themselves.
        tweet = re.sub(r"\d+\S+", ' ', tweet) # Removes numbers, as well as cases like the "th" in "14th".
        # tweet = ''.join([punc for punc in tweet if not punc in punctuations]) # Removes the punctuation defined above.
        # tweet = tweet.lower() # Turning the tweets lowercase real quick for later use.
        df.loc[i, "tweet"] = tweet

    return df

@task(name="Transform with tweetnlp", log_prints=True)
async def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform with tweetnlp"""

    # Topic Classification.
    # The aim of this task is, given a tweet to assign topics related to its content. 
    classification_model = tweetnlp.load_model('topic_classification', multi_label=False)  

    # Named Entity Recognition.
    # This module consists of a named-entity recognition (NER) model specifically trained for tweets.
    ner_model = tweetnlp.load_model('ner')

    for i in range(len(df)):
        if i % 1000 == 0:
            print(f"i={i}")

        text = df.loc[i, "tweet"]

        # Topic Classification.
        dict = classification_model.topic(text, return_probability=True)
        topic = dict['label']
        df.loc[i, "topic"] = topic

        try:
            science_technology_probability = dict['probability']['science_&_technology']
        except KeyError:
            science_technology_probability = 0
        df.loc[i, "science_technology_probability"] = science_technology_probability

        # Named Entity Recognition.
        list = ner_model.ner(text, return_probability=True)
        sorted_list = sorted(list, key=lambda x: x['probability'], reverse=True)

        # Get product with highest probability.
        product = "None"
        for item in sorted_list:
            if item['type'] == 'product':
                product = item['entity'].strip()
                break
        df.loc[i, "product"] = product

        # Get corporation with highest probability.
        corporation = "None"
        for item in sorted_list:
            if item['type'] == 'corporation':
                corporation = item['entity'].strip()
                break
        df.loc[i, "corporation"] = corporation

    return df

@task(name="Write DataFrame to BiqQuery table", log_prints=True)
def write_bq(config: dict, df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery table"""
    gcp_credentials = config['gcp_credentials']
    project_id = config['project_id']
    bq_dataset = config['bq_dataset']
    bq_table = config['bq_table']

    gcp_credentials_block = GcpCredentials.load(gcp_credentials)

    # See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_gbq.html
    df.to_gbq(
        destination_table = bq_dataset + "." + bq_table,
        project_id = project_id,
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = 'replace'
    )

@flow(name="gcs-to-bq", log_prints=True)
def gcs_to_bq() -> None:
    """Main ETL flow to load data into Big Query"""
    config = read_config()
    path = get_gcs_path(config)
    df = read_from_gcs(path)
    df_clean = clean_tweets(df)
    df_transformed = transform(df)
    write_bq(config, df)

