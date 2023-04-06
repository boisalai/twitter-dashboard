import configparser
import pathlib
import pandas as pd
import asyncio
import os

from config import *
from pathlib import Path

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

# Import Spark NLP
# from sparknlp.base import *
# from sparknlp.annotator import *
# from sparknlp.pretrained import PretrainedPipeline
# import sparknlp

@task(name="Read Configuration File", log_prints=True)
def read_config() -> dict:
    config = get_config()
    return config

@task(name="Download data from GCS", retries=3, log_prints=True)
def extract_from_gcs(config: dict) -> Path:
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

@task(name="Start Spark NLP Session", log_prints=True)
async def start_sparknlp() -> object:
    # Start SparkSession with Spark NLP
    # start() functions has 3 parameters: gpu, apple_silicon, and memory
    # sparknlp.start(gpu=True) will start the session with GPU support
    # sparknlp.start(apple_silicon=True) will start the session with macOS M1 & M2 support
    # sparknlp.start(memory="16G") to change the default driver memory in SparkSession
    spark = sparknlp.start()

    print("Spark NLP version", sparknlp.version()) # Spark NLP version 4.3.2
    print("Apache Spark version:", spark.version) # Apache Spark version: 3.3.2

    return spark

@task(name="Download a pre-trained pipeline", log_prints=True)
async def fetch_pretrained(spark: object) -> object:
    # Download a pre-trained pipeline.
    # pipeline = PretrainedPipeline('explain_document_dl', lang='en')

    # Load pretrained pipeline from local disk
    # pipeline_local = PretrainedPipeline.from_disk('/root/cache_pretrained/explain_document_ml_en_4.0.0_3.0_1656066222624')

    return pipeline

# See https://colab.research.google.com/github/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/1.SparkNLP_Basics.ipynb
@task(name="Transform dataFrame", log_prints=True)
async def transform(pipeline: object, df: pd.DataFrame) -> pd.DataFrame:
    """Transforme"""

    # Your testing dataset
    text = """
    The Mona Lisa is a 16th century oil painting created by Leonardo.
    It's held at the Louvre in Paris.
    """

    # Annotate your testing dataset
    result = pipeline.annotate(text)

    # What's in the pipeline
    print(list(result.keys()))
    # Output: ['entities', 'stem', 'checked', 'lemma', 'document',
    #         'pos', 'token', 'ner', 'embeddings', 'sentence']

    # Check the results
    print(result['entities'])
    # Output: ['Mona Lisa', 'Leonardo', 'Louvre', 'Paris']

    return df

@task(name="Write DataFrame to BiqQuery", log_prints=True)
def write_bq(config: dict, df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials = config['gcp_credentials']
    project_id = config['project_id']
    bq_dataset = config['bq_dataset']
    bq_table = config['bq_table']

    gcp_credentials_block = GcpCredentials.load(gcp_credentials)

    df.to_gbq(
        destination_table = bq_dataset + "." + bq_table,
        project_id = project_id,
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = "append",
    )

@flow(name="gcs-to-bq", log_prints=True)
def gcs_to_bq() -> None:
    """Main ETL flow to load data into Big Query"""
    config = read_config()
    path = extract_from_gcs(config)
    df = read_from_gcs(path)
    # spark = start_sparknlp()
    # pipeline = fetch_pretrained(spark)
    # df_transformed = transform(pipeline, df)
    write_bq(config, df)

