import os
from flask import current_app
from .models import db, Image
from .config import Config
from .file_utils import open_file
from google.cloud import storage
import json

def update_image_predictions_gcp(field_id, batch_id):
    """Fetch predictions from GCP bucket and update database."""
    try:
        # Set up the Google Cloud Storage client and get the bucket
        storage_client = storage.Client()
        bucket_name = Config.GCS_BUCKET_NAME
        bucket = storage_client.bucket(bucket_name)

        # Construct the path to the predictions file
        predictions_file_path = f"userdata/{field_id}/{batch_id}/predictions_with_confidences.json"
        blob = bucket.blob(predictions_file_path)
        predictions_data = blob.download_as_text()

        # Parse predictions JSON
        predictions = json.loads(predictions_data)
        
        for prediction in predictions:
            filename = prediction['Id'].split('/')[-1]
            image = Image.query.filter_by(filename=filename, batch_id=batch_id).first()
            if image:
                confidence_levels = eval(prediction['Class Confidence Levels'])
                image.healthy = confidence_levels.get('Healthy', 0)
                image.rice_blast = confidence_levels.get('Rice Blast', 0)
                image.brown_spot = confidence_levels.get('Brown Spot', 0)
                image.label = prediction['Class Prediction']
                db.session.commit()
        return True, "Predictions updated successfully."
    except FileNotFoundError:
        return False, f"The predictions file for field {field_id} and batch {batch_id} was not found."
    except Exception as e:
        db.session.rollback()
        return False, f"An error occurred while updating image predictions: {str(e)}"
    
def update_image_status_to_predicting(batch_id):
    """Update the label of all images in a batch to 'predicting'."""
    try:
        images = Image.query.filter_by(batch_id=batch_id).all()
        for image in images:
            image.label = 'predicting'
        db.session.commit()
        return True, "Image status updated to predicting."
    except Exception as e:
        db.session.rollback()
        return False, f"Error updating image status: {str(e)}"

def update_image_predictions(field_id, batch_id):
    try:
        predictions = open_file(field_id, batch_id, 'predictions_with_confidences.json', 'r')
        for prediction in predictions:
            filename = prediction['Id'].split('/')[-1]
            image = Image.query.filter_by(filename=filename, batch_id=batch_id).first()
            if image:
                confidence_levels = eval(prediction['Class Confidence Levels'])
                image.healthy = confidence_levels.get('Healthy', 0)
                image.rice_blast = confidence_levels.get('Rice Blast', 0)
                image.brown_spot = confidence_levels.get('Brown Spot', 0)

                # Update label based on 'Class Prediction'
                image.label = prediction['Class Prediction']

                db.session.commit()
    except FileNotFoundError:
        current_app.logger.error(f"The predictions file for field {field_id} batch {batch_id} was not found.")
    except Exception as e:
        current_app.logger.error(f"An error occurred while updating image predictions: {e}")

def run_model(field_id, batch_id):
    hybrid_model_path = os.path.join(Config.MODEL_DIR, Config.MODEL_SCRIPT)
    #subprocess.run(['python', hybrid_model_path, str(field_id), str(batch_id)], check=True)
    current_app.logger.info('Model is running...')
    update_image_status_to_predicting(batch_id)