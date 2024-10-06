import os
from google.cloud import secretmanager, storage
from google.oauth2 import service_account
import json

def get_secret(secret_name):
    """Retrieve a secret value from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    project_id = "tidy-nomad-415320"
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    secret_string = response.payload.data.decode('UTF-8')
    return secret_string

def setup_google_credentials():
    """
    Fetches service account credentials from Google Cloud Secret Manager and returns
    a credentials object.
    """
    project_id = 'tidy-nomad-415320'
    secret_id = 'main-service-acc-key'
    service_account_info = get_secret(secret_id)

    # Load the service account into a dictionary, which can then be used to create a credentials object
    service_account_info_json = json.loads(service_account_info)
    
    # Create a credentials object from the service account info
    credentials = service_account.Credentials.from_service_account_info(service_account_info_json)
    
    return credentials
    
class Config:
    #credentials = setup_google_credentials()

    PROJECT = 'tidy-nomad-415320'
    PROJECT_ID = '57814283461'

    # Determine the environment mode
    ENV_MODE = os.getenv('ENV_MODE', 'development')
    
    # General configurations
    GCS_BUCKET_NAME = 'userdata-tidy-nomad-415320'
    #UPLOAD_FOLDER = 'userdata' if ENV_MODE == 'development' else GCS_BUCKET_NAME
    UPLOAD_FOLDER = 'userdata'
    ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}

    # Database and secrets
    if ENV_MODE == 'production':
        # Production environment using Secret Manager
        SECRET_KEY = get_secret('flask_secret_key')
        DB_USER = get_secret('DB_USER')
        DB_PASS = get_secret('DB_PASS')
        DB_HOST = get_secret('DB_HOST')
        DB_NAME = get_secret('DB_NAME')
        DB_CONN_NAME = get_secret('DB_CONN_NAME')
        SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@/{DB_NAME}?unix_socket=/cloudsql/{DB_CONN_NAME}"
 
        # Set up Google Cloud Storage client
        storage_client = storage.Client()
        gcs_bucket = storage_client.bucket(GCS_BUCKET_NAME)

    else:
        # Local development environment using environment variables
        SECRET_KEY = os.getenv('SECRET_KEY', 'your_default_secret_key')
        SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI', 'sqlite:///database.db')

    # Hybrid Model
    MODEL_DIR = 'ai'
    MODEL_SCRIPT = 'hybrid_model.py'
    MODEL_H5 = 'Best_DenseNet121_Hybrid_Model.h5'

    # Visual Crossing Weather API Key
    # API Keys: "UJJ79UT38U9BAYFRRFFTSEQYX"  # or "K9PGACG8N2SYC5JAWHG9X7KQ9"
    VC_API_KEY = "K9PGACG8N2SYC5JAWHG9X7KQ9"
    VC_API_ENDPOINT = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

    # Google Earth Engine
    GEE_PROJECT = "tidy-nomad-415320"