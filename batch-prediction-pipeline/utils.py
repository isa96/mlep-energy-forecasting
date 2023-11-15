import pandas as pd
import json
import logging
import joblib
from pathlib import Path
from typing import Union, Optional
from .settings import OUTPUT_DIR, SETTING

from google.cloud import storage


def load_model(path: Union[str, Path]):
    """
    Template for loading a model.

    Args:
        model_path: Path to the model.

    Returns: Loaded model.
    """
    return joblib.load(path)


def get_logger(name: str) -> logging.Logger:
    """
    Template get a logger
    Args:
        name: name of the logger
    return: Logger
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)
    return logger


def save_json(data: dict, file_name: str, save_dir: str = OUTPUT_DIR):
    """
    Save a dictionary as a JSON file.

    Args:
        data: data to save.
        file_name: Name of the JSON file.
        save_dir: Directory to save the JSON file.
    """
    data_path = Path(save_dir) / file_name
    with open(data_path, "w") as f:
        json.dump(data, f)


def load_json(file_name: str, save_dir: str = OUTPUT_DIR) -> dict:
    """
    Load a JSON file.

    Args:
        file_name: Name of the JSON file.
        save_dir: Directory of the JSON file.

    Returns: Dictionary with the data.
    """

    data_path = Path(save_dir) / file_name
    if not data_path.exists():
        raise FileNotFoundError(f"Cached JSON from {data_path} does not exist.")

    with open(data_path, "r") as f:
        return json.load(f)

def get_bucket(
    bucket_name: str=SETTING["GOOGLE_CLOUD_BUCKET_NAME"],
    bucket_project: str=SETTING["GOOGLE_CLOUD_PROJECT_NAME"],
    json_credentials_path: str=SETTING["GOOGLE_CLOUD_SERVICE_ACCOUNT_JSON_PATH"]
) -> storage.Bucket:
    """Get a Google Cloud Storage bucket.

    This function returns a Google Cloud Storage bucket that can be used to upload and download
    files from Google Cloud Storage.

    Args:
        bucket_name : str
            The name of the bucket to connect to.
        bucket_project : str
            The name of the project in which the bucket resides.
        json_credentials_path : str
            Path to the JSON credentials file for your Google Cloud Project.

    Returns
        storage.Bucket
            A storage bucket that can be used to upload and download files from Google Cloud Storage.
    """
    storage_client = storage.Client.from_service_account_json(
        json_credentials_path=json_credentials_path,
        project=bucket_project
    )
    bucket = storage.Client.bucket(bucket_name=bucket_name)

    return bucket


def write_blob_to(bucket: storage.Bucket, blob_name: str, data: pd.DataFrame):
    """Write a dataframe to a GCS bucket as a parquet file.

    Args:
        bucket (google.cloud.storage.Bucket): The bucket to write to.
        blob_name (str): The name of the blob to write to. Must be a parquet file.
        data (pd.DataFrame): The dataframe to write to GCS.
    """

    blob = bucket.blob(blob_name=blob_name)
    with blob.open("wb") as f:
        data.to_parquet(f)


def read_blob_from(bucket: storage.Bucket, blob_name: str) -> Optional[pd.DataFrame]:
    """Reads a blob from a bucket and returns a dataframe.

    Args:
        bucket: The bucket to read from.
        blob_name: The name of the blob to read.

    Returns:
        A dataframe containing the data from the blob.
    """

    blob = bucket.blob(blob_name=blob_name)
    if not blob.exists():
        return None

    with blob.open("rb") as f:
        return pd.read_parquet(f)