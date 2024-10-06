import os
import pandas as pd
import requests
from google.cloud import storage, pubsub_v1
import csv
from datetime import datetime, timedelta
import traceback
import json
import shutil
import base64
from collections import OrderedDict

def fetch_weather_data(event, context):
    """Cloud Function triggered by the message from metadata_extractor function."""
    print("Raw event data:", event)

    # Decode and process the pub/sub message
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print("Decoded message:", pubsub_message)
    message_data = json.loads(pubsub_message)
    print("Message data:", message_data)

    bucket_name = message_data["bucket"]
    field_id = message_data["field_id"]
    batch_id = message_data["batch_id"]

    try:
        # Setup paths and download the metadata CSV to /tmp directory
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        local_dir = f"/tmp/{field_id}/{batch_id}/"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "image_metadata.csv")
        blob = bucket.blob(f"userdata/{field_id}/{batch_id}/image_metadata.csv")
        blob.download_to_filename(local_path)

        df = pd.read_csv(local_path)
        weather_csv_path = f"/tmp/weather_data_{field_id}_{batch_id}.csv"

        coordinates_and_dates = []

        for _, row in df.iterrows():
            lat = round(row['Latitude'], 2)
            lon = round(row['Longitude'], 2)
            date_created = row['Date']
            coordinates_and_dates.append((lat, lon, date_created))
        
        # Getting rid of duplicate tuples
        # Using an OrderedDict to remove duplicates while maintaining order. Keys are the tuples.
        unique_coordinates_and_dates_ordered = list(OrderedDict.fromkeys(coordinates_and_dates))
        
        # Preparing the CSV file at the specified file path
        with open(weather_csv_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Latitude", "Longitude", "Date", "Avg Temp 14d", "Avg Humidity 14d", "Total Precipitation 14d", "Avg Wind Speed 14d"])

            for latitude, longitude, date_str in unique_coordinates_and_dates_ordered:
                # Calculating the date range to obtain the data
                end_date = datetime.strptime(date_str, '%Y-%m-%d')
                start_date = end_date - timedelta(days=14)

                # Formatting the dates for the API request
                start_date_str = start_date.strftime('%Y-%m-%d')
                end_date_str = (end_date - timedelta(days=1)).strftime('%Y-%m-%d')  # Excluding the given date itself

                # Making the API request
                response = requests.get(
                    f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{start_date_str}/{end_date_str}?unitGroup=metric&include=days&key={os.getenv('VC_API_KEY')}&contentType=json"
                )
                weather_data = response.json()

                # Initializing variables for the calculations
                total_temp = 0
                total_humidity = 0
                total_precipitation = 0
                total_wind_speed = 0
                days_counted = 0

                # Accumulating the weather data
                for day in weather_data['days']:
                    total_temp += day['temp']
                    total_humidity += day['humidity']
                    total_precipitation += day.get('precip', 0)
                    total_wind_speed += day['windspeed']
                    days_counted += 1

                # Calculating the averages
                avg_temp = total_temp / days_counted if days_counted else 0
                avg_humidity = total_humidity / days_counted if days_counted else 0
                avg_wind_speed = total_wind_speed / days_counted if days_counted else 0

                writer.writerow([latitude, longitude, date_str, avg_temp, avg_humidity, total_precipitation, avg_wind_speed])

        # Upload the CSV back to Cloud Storage
        output_blob = bucket.blob(f"userdata/{field_id}/{batch_id}/weather_data.csv")
        output_blob.upload_from_filename(weather_csv_path)

        # Cleanup temporary files
        os.remove(local_path)
        os.remove(weather_csv_path)
        print(f"Weather data processed and saved for batch {batch_id} in field {field_id}.")

        # Publish a message to the weather-data-fetched topic
        publisher = pubsub_v1.PublisherClient()
        project_id = "tidy-nomad-415320"
        topic_name = "weather-data-fetched"
        topic_path = publisher.topic_path(project_id, topic_name)
        data = {"bucket": bucket_name, "field_id": field_id, "batch_id": batch_id}
        message = json.dumps(data).encode("utf-8")
        future = publisher.publish(topic_path, data=message)
        future.result()

        print("Published message to weather-data-fetched topic.")

    except Exception as e:
        print(f"Error processing weather data: {traceback.format_exc()}")

    # Safely remove all temporary files after processing and uploading CSV
    shutil.rmtree(local_dir, ignore_errors=True)