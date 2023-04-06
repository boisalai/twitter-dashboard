import pathlib
import pandas as pd
import asyncio
import re 
import html

import spacy
from spacy import displacy
from collections import Counter
import en_core_web_sm

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
        tweet = ''.join([punc for punc in tweet if not punc in punctuations]) # Removes the punctuation defined above.
        tweet = tweet.lower() # Turning the tweets lowercase real quick for later use.
        df.loc[i, "tweet"] = tweet

    return df

@task(name="Transform with spacy", log_prints=True)
async def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform with spacy"""
    # nlp = en_core_web_sm.load()
    nlp = spacy.load('en_core_web_sm')

    for i in range(len(df)):
        if i % 1000 == 0:
            print(f"i={i}")

        text = df.loc[i, "tweet"]
        doc = nlp(text)

        if len(doc.ents) > 0:
            product = str(doc.ents[0])
        else:
            product = None

        df.loc[i, "product"] = product

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
    df_transformed = transform(df_clean)
    write_bq(config, df_transformed)

