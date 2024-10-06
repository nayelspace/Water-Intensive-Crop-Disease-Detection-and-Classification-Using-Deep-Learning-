import os
import pandas as pd
import numpy as np
from google.cloud import storage, pubsub_v1
import traceback
import json
import shutil
import base64
import requests
import google.auth
from google.auth.transport.requests import Request

def predict(data):
    """Function to send data to the Vertex AI endpoint and get the prediction."""
    project_id = "57814283461"
    endpoint_id = "4526755342251458560"
    url = f"https://us-west1-aiplatform.googleapis.com/v1/projects/{project_id}/locations/us-west1/endpoints/{endpoint_id}:predict"

    # Authenticate the request
    credentials, project = google.auth.default()
    auth_req = Request()
    credentials.refresh(auth_req)

    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json"
    }

    # Make the prediction request
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def consolidate_datasets(event, context):
    """Cloud Function triggered by messages from remote-sensing-data-fetched and weather-data-fetched topics."""
    try:
        print("Raw event data:", event)
        
        # Decode and process the pub/sub message
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        print("Decoded message:", pubsub_message)
        message_data = json.loads(pubsub_message)
        print("Message data:", message_data)

        # Extract relevant information from the message
        bucket_name = message_data["bucket"]
        field_id = message_data["field_id"]
        batch_id = message_data["batch_id"]
        
        # Define the base directory where the files are stored
        base_path = f"userdata/{field_id}/{batch_id}"

        # Initialize Google Cloud Storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Check for the existence of all required files
        files = ['image_metadata.csv', 'remote_sensing_data.csv', 'weather_data.csv']
        files_exist = all(bucket.blob(f"{base_path}/{file}").exists() for file in files)

        if files_exist:
            # Initialize Google Cloud Storage client
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            
            # Download the required files
            local_paths = {}
            for file in files:
                blob = bucket.blob(f"{base_path}/{file}")
                local_path = f"/tmp/{file}"
                blob.download_to_filename(local_path)
                local_paths[file] = local_path

            # Load datasets
            metadata_df = pd.read_csv(local_paths['image_metadata.csv'])
            weather_df = pd.read_csv(local_paths['weather_data.csv'])
            modis_df = pd.read_csv(local_paths['remote_sensing_data.csv'])
            
            # Creating rounded latitude and longitude in metadata_df and modis_df for matching to the weather data (since latitude and longitude are rounded to 2 decimal places in weather_df)
            metadata_df['Rounded_Latitude'] = metadata_df['Latitude'].round(2)
            metadata_df['Rounded_Longitude'] = metadata_df['Longitude'].round(2)
            modis_df['Rounded_Latitude'] = modis_df['Latitude'].round(2)
            modis_df['Rounded_Longitude'] = modis_df['Longitude'].round(2)

            # Ensuring the 'Date' columns are in the same format (YYYY-MM-DD)
            metadata_df['Date'] = pd.to_datetime(metadata_df['Date']).dt.strftime('%Y-%m-%d')
            weather_df['Date'] = pd.to_datetime(weather_df['Date']).dt.strftime('%Y-%m-%d')
            modis_df['Date'] = pd.to_datetime(modis_df['Date']).dt.strftime('%Y-%m-%d')

            # Merging metadata_df with weather_df
            combined_part_df = pd.merge(metadata_df, weather_df, left_on=['Rounded_Latitude', 'Rounded_Longitude', 'Date'], right_on=['Latitude', 'Longitude', 'Date'], how='left', suffixes=('', '_weather'))

            # Dropping the rounded and duplicate columns from the weather_df merge
            combined_part_df.drop(columns=['Rounded_Latitude', 'Rounded_Longitude', 'Latitude_weather', 'Longitude_weather'], inplace=True)

            # Merging the combined_part_df with modis_df based on the exact Latitude, Longitude, and Date match
            df = pd.merge(combined_part_df, modis_df, on=['Latitude', 'Longitude', 'Date'], how='left', suffixes=('', '_modis'))

            # Dropping the rounded and duplicate columns from the final merge
            df.drop(columns=['Rounded_Latitude', 'Rounded_Longitude'], inplace=True)

            # Indicators for remote sensing data

            # Adding the "NDVI 1 Decrease" column based on comparing "NDVI MODIS" and "NDVI - 1 MODIS"
            df['NDVI 1 Decrease'] = np.where(df['NDVI MODIS'] < df['NDVI - 1 MODIS'], 1, 0)

            # Adding the "NDVI 2 Decrease" column based on comparing "NDVI MODIS" and "NDVI - 2 MODIS"
            df['NDVI 2 Decrease'] = np.where(df['NDVI MODIS'] < df['NDVI - 2 MODIS'], 1, 0)

            # Adding the "EVI 1 Decrease" column based on comparing "EVI MODIS" and "EVI - 1 MODIS"
            df['EVI 1 Decrease'] = np.where(df['EVI MODIS'] < df['EVI - 1 MODIS'], 1, 0)

            # Adding the "EVI 2 Decrease" column based on comparing "EVI MODIS" and "EVI - 2 MODIS"
            df['EVI 2 Decrease'] = np.where(df['EVI MODIS'] < df['EVI - 2 MODIS'], 1, 0)
            
            # Save the consolidated dataset to a CSV
            combined_data_path = f"/tmp/combined_data_{field_id}_{batch_id}.csv"
            df.to_csv(combined_data_path, index=False)
            
            # Upload the CSV back to Cloud Storage
            output_blob = bucket.blob(f"{base_path}/combined_data.csv")
            output_blob.upload_from_filename(combined_data_path)

            print(f"Combined dataset saved to {base_path}/combined_data.csv.")

            # Prepare data for prediction
            predict_data = {"instances": [{"field_id": field_id, "batch_id": batch_id, "bucket": bucket_name}]}
            print("Sending the following data for prediction:", predict_data)
            
            prediction_result = predict(predict_data)
            print("Prediction result:", prediction_result)
            
            # Cleanup temporary files
            for path in local_paths.values():
                os.remove(path)
            os.remove(combined_data_path)

            # Publish a message to the topic datasets-consolidated
            publisher = pubsub_v1.PublisherClient()
            project_id = "tidy-nomad-415320"
            topic_name = "datasets-consolidated"
            topic_path = publisher.topic_path(project_id, topic_name)
            data = {"bucket": bucket_name, "field_id": field_id, "batch_id": batch_id}
            message = json.dumps(data).encode("utf-8")
            future = publisher.publish(topic_path, data=message)
            future.result()

            print("Published message to datasets-consolidated topic.")

        else:
            print("Not all necessary files are available for processing.")

    except Exception as e:
        print(f"Error in consolidating datasets: {traceback.format_exc()}")

     # After processing and uploading CSV remove all temporary files
    try:
        shutil.rmtree(local_paths[file], ignore_errors=True)
    except Exception as e:
        print(f"Error cleaning up temporary files: {e}")