import configparser
import pandas as pd
import pathlib
import asyncio

from config import *
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta

import snscrape.modules.twitter as sntwitter

"""
See Best practices for using async in Prefect 2.0
https://discourse.prefect.io/t/best-practices-for-using-async-in-prefect-2-0/1361
"""

@task(name="Read Configuration File", log_prints=True)
def read_config() -> dict:
    config = get_config()
    return config

@task(name="Extract posts from Twitter", log_prints=True)
async def extract_twitter(config: dict) -> pd.DataFrame:
    """Create posts object for Reddit instance"""
    try:
        # Determine the start and end dates of the last year.
        end_date = datetime.today()
        start_date = end_date - timedelta(days=365)

        # Convert these dates to string.
        end_date = end_date.strftime('%Y-%m-%d')
        start_date = start_date.strftime('%Y-%m-%d')

        # Create the query.
        hashtag = config['hashtag']
        query = hashtag + " since:" + start_date + " until:" + end_date
        print(query)

        # Creating list to append tweets data to.
        data = []

        # Set limit.
        limit = config['limit']
        if limit.isdigit():
            limit = int(limit)
        else:
            limit = 15000

        # Using TwitterSearchScraper to scrape data and append tweets to list.
        items = sntwitter.TwitterSearchScraper(query).get_items()
        for i, tweet in enumerate(items):
            if i > limit:
                break
            data.append([tweet.user.username, tweet.date, tweet.likeCount, 
                tweet.retweetCount, tweet.replyCount, tweet.sourceLabel, 
                tweet.rawContent, tweet.url])
            if i % 1000 == 0:
                print(f"i={i}")
    
        print(f"len(data)={len(data)}")

        # Creating a dataframe to load the list.
        tweets_df = pd.DataFrame(data, 
            columns=["username", "created_utc", "likes", "retweets", "replies",
                     "source", "content", "url"])

        return tweets_df
    except Exception as e:
        print(e)

@task(name="Clean dataset", log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataset"""
    # Cast created to UTC.
    df['created_utc'] = pd.to_datetime(df['created_utc'])

    # Cast some fields.
    # Int16: (-32,768 to +32,767)
    # Int32: (-2,147,483,648 to +2,147,483,647)
    # Int64: (-9,223,372,036,854,775,808 to +9,223,372,036,854,775,807)
    df = df.astype({'likes': 'int32', 'retweets': 'int32', 'replies': 'int32'})

    print(f"Dataframe info:")
    print(df.info())
    print(f"The earliest post: {df['created_utc'].min()}")
    print(f"The latest post: {df['created_utc'].max()}")
    return df

@task(name="Write DataFrame out locally as parquet file", log_prints=True)
def write_local(config: dict, df: pd.DataFrame) -> Path:
    """Write DataFrame out locally as parquet file"""
    bucket_filename = config['bucket_filename']
    path = Path(f"data/{bucket_filename}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    path = Path(path).as_posix()
    df.to_parquet(path, compression="gzip")
    print(f"path={path}")
    return path

@task(name="Test dataframe schema", log_prints=True)
def test_dataframe_schema(path: Path) -> None:
    df = pd.read_parquet(path)
    expected_columns = ["username", "created_utc", "likes", "retweets", 
                        "replies", "source", "content", "url"]
    assert list(df.columns) == expected_columns

@task(name="Write to GCS bucket", log_prints=True)
def write_gcs(config: dict, path: Path) -> None:
    gcs_bucket_block_name = config['gcs_bucket_block_name']
    gcs_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow(name="web-to-gcs", log_prints=True)
def web_to_gcs() -> None:
    config = read_config()
    df = extract_twitter(config)
    df_clean = clean(df)
    path = write_local(config, df_clean)
    test_dataframe_schema(path)
    write_gcs(config, path)

