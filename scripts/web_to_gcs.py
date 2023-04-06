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
async def extract_twitter() -> pd.DataFrame:
    """Create posts object for Reddit instance"""
    try:
        end_date = datetime.today()
        start_date = end_date - timedelta(days=365)
        print(end_date.strftime('%Y-%m-%d'))
        print(start_date.strftime('%Y-%m-%d'))

        hashtag = "#DataEngineering"
        query = hashtag + " since:" + start_date.strftime('%Y-%m-%d') + " until:" + end_date.strftime('%Y-%m-%d')
        print(query)

        items = sntwitter.TwitterSearchScraper(query).get_items()

        # Creating list to append tweet data to
        data = []

        # Using TwitterSearchScraper to scrape data and append tweets to list
        for i, tweet in enumerate(items):
            if i>15000:
                break
            data.append([tweet.user.username, tweet.date, tweet.likeCount, tweet.retweetCount, tweet.replyCount, tweet.sourceLabel, tweet.rawContent, tweet.url])
    
        print(f"len(data)={len(data)}")

        # Creating a dataframe to load the list
        tweets_df = pd.DataFrame(data, 
            columns=["username", "created_utc", "likes", "retweets", "replies",
                     "source", "content", "url"])

        return tweets_df
    except Exception as e:
        print(e)

@task(name="Clean dataset", log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataset"""
    # Convert epoch to UTC.
    # df['created_utc'] = pd.to_datetime(df['created_utc'])

    # Convert some fields to bool.
    # df = df.astype({'over_18': bool, 'edited': bool, 'spoiler': bool, 'stickied': bool})

    print(f"Dataframe info:")
    print(df.info())
    print(f"The earliest post: {df['created_utc'].min()}")
    print(f"The latest post: {df['created_utc'].max()}")
    return df

@task(name="Write DataFrame out locally as parquet file", log_prints=True)
def write_local(df: pd.DataFrame, bucket_filename: str) -> Path:
    """Write DataFrame out locally as parquet file"""
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
    expected_columns = ["username", "created_utc", "likes", "retweets", "replies",
                        "source", "content", "url"]
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
    df = extract_twitter()
    df_clean = clean(df)
    bucket_filename = config['bucket_filename']
    path = write_local(df_clean, bucket_filename)
    test_dataframe_schema(path)
    write_gcs(config, path)

