import configparser
import pathlib

def get_config() -> dict:
    """Read Configuration File"""
    parser = configparser.ConfigParser()
    script_path = pathlib.Path(__file__).parent.resolve()
    config_file = "configuration.conf"
    parser.read(f"{script_path}/{config_file}")

    config = dict()

    # GCP configurations.
    config['project_id'] = parser.get("gcp_config", "project_id")
    config['data_lake_bucket'] = parser.get("gcp_config", "data_lake_bucket")
    config['bucket_filename'] = parser.get("gcp_config", "bucket_filename")
    config['bq_dataset'] = parser.get("gcp_config", "bq_dataset")
    config['bq_table'] = parser.get("gcp_config", "bq_table")

    # Prefect configurations.
    config['gcp_credentials'] = parser.get("prefect_config", "gcp_credentials")
    config['gcs_bucket_block_name'] = parser.get("prefect_config", "gcs_bucket_block_name")

    # Twitter configurations.
    config['hashtag'] = parser.get("prefect_config", "hashtag")
    config['limit'] = parser.get("prefect_config", "limit")

    return config