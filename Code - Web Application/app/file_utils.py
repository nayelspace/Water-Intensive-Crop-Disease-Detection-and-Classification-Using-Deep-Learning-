import os
from flask import current_app
from app.config import Config
from google.cloud import storage
import json
from werkzeug.utils import secure_filename
import logging

def save_file(file, directory, filename):
    """Save a file either to a local path or to Google Cloud Storage."""
    try:
        sanitized_filename = secure_filename(filename)
        if Config.ENV_MODE == 'development':
            filepath = os.path.join(directory, sanitized_filename)
            file.save(filepath)
            logging.info(f"File saved locally at {filepath}")
        else:
            # Ensure the Google Cloud Storage bucket is initialized
            if not hasattr(Config, 'gcs_bucket') or not Config.gcs_bucket:
                storage_client = storage.Client()
                Config.gcs_bucket = storage_client.bucket(Config.GCS_BUCKET_NAME)
            
            # Google Cloud Storage
            bucket = Config.gcs_bucket
            blob = bucket.blob(os.path.join(directory, sanitized_filename))
            blob.upload_from_file(file, content_type=file.content_type)
            logging.info(f"File uploaded to GCS at {blob.name}")
    except Exception as e:
        logging.error(f"Failed to save file {sanitized_filename}: {e}", exc_info=True)
        raise RuntimeError(f"Failed to save file {sanitized_filename}: {e}")

def create_directory(directory):
    """Create a directory either locally or in Google Cloud Storage."""
    if Config.ENV_MODE == 'development':
        os.makedirs(directory, exist_ok=True)
    else:
        try:
            # Ensure the storage client and bucket are correctly initialized
            if not hasattr(Config, 'gcs_bucket') or not Config.gcs_bucket:
                storage_client = storage.Client()
                Config.gcs_bucket = storage_client.bucket(Config.GCS_BUCKET_NAME)
            bucket = Config.gcs_bucket
            blob = bucket.blob(os.path.join(directory, 'placeholder'))
            blob.upload_from_string('', content_type='text/plain')
        except Exception as e:
            logging.error(f'Failed to create directory in GCS: {e}', exc_info=True)
            raise e

def list_files(directory):
    """List files in a directory either locally or in Google Cloud Storage."""
    if Config.ENV_MODE == 'development':
        return os.listdir(directory)
    else:
        bucket = Config.gcs_bucket
        blobs = bucket.list_blobs(prefix=directory)
        return [blob.name for blob in blobs if '/' not in blob.name[len(directory):]]

def read_file(directory, filename):
    """Read a file either from a local path or from Google Cloud Storage."""
    if Config.ENV_MODE == 'development':
        with open(os.path.join(directory, filename), 'rb') as f:
            return f.read()
    else:
        bucket = Config.gcs_bucket
        blob = bucket.blob(os.path.join(directory, filename))
        return blob.download_as_bytes()

def get_file_path(field_id, batch_id, filename):
    """Construct file path depending on the environment."""
    if Config.ENV_MODE == 'development':
        return os.path.join(Config.UPLOAD_FOLDER, str(field_id), str(batch_id), filename)
    return f"{Config.UPLOAD_FOLDER}/{field_id}/{batch_id}/{filename}"

def open_file(field_id, batch_id, filename, mode='r'):
    """Open a file with consideration for the environment."""
    file_path = get_file_path(field_id, batch_id, filename)
    try:
        if Config.ENV_MODE == 'development':
                with open(file_path, mode) as file:
                    return json.load(file)
        else:
            bucket = Config.gcs_bucket
            blob = bucket.blob(file_path)
            if mode == 'r':
                return json.loads(blob.download_as_text())
            else:
                return blob.open(mode)
    except Exception as e:
        current_app.logger.error(f"Failed to open file {file_path}: {e}")
        raise
        
