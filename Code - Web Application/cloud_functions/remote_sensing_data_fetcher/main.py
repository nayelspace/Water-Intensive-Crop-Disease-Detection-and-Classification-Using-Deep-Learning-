import ee
import pandas as pd
import os
import json
import csv
from google.cloud import storage, pubsub_v1
import traceback
import shutil
import base64
from collections import OrderedDict

def get_modis_values(latitude, longitude, date_str):
    point = ee.Geometry.Point([longitude, latitude])
    target_date = ee.Date(date_str)

    modis = ee.ImageCollection('MODIS/006/MOD13Q1')\
        .filterBounds(point)\
        .filterDate('2000-01-01', target_date)\
        .select(['NDVI', 'EVI'])\
        .sort('system:time_start', False)\
        .limit(3)

    ndvi_values = []
    evi_values = []

    for i in range(3):
        image = modis.toList(3).get(i)
        image = ee.Image(image)

        # Directly using the pre-calculated NDVI and EVI, scaled to actual values
        ndvi_val = image.select('NDVI').multiply(0.0001).reduceRegion(ee.Reducer.first(), point, 250).get('NDVI').getInfo()
        evi_val = image.select('EVI').multiply(0.0001).reduceRegion(ee.Reducer.first(), point, 250).get('EVI').getInfo()

        ndvi_values.append(ndvi_val)
        evi_values.append(evi_val)

    # Padding missing values for NDVI and EVI if fewer than 3 images are available
    while len(ndvi_values) < 3:
        ndvi_values.append(None)
    while len(evi_values) < 3:
        evi_values.append(None)

     # Combining NDVI and EVI values for the CSV
    values_list = [latitude, longitude, date_str] + ndvi_values + evi_values

    return values_list

def write_to_csv(filename, data):
    headers = ["Latitude", "Longitude", "Date",
               "NDVI MODIS", "NDVI - 1 MODIS", "NDVI - 2 MODIS",
               "EVI MODIS", "EVI - 1 MODIS", "EVI - 2 MODIS"]

    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

def fetch_remote_sensing_data(event, context):
    """Triggered by the message from metadata_extractor cloud function"""
    print("Raw event data:", event['data'])
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print("Decoded message:", pubsub_message)
    message_data = json.loads(pubsub_message)
    print("Message data:", message_data)

    bucket_name = message_data["bucket"]
    field_id = message_data["field_id"]
    batch_id = message_data["batch_id"]

    try:
        # Initialize Earth Engine and Storage Client
        ee.Initialize()
        storage_client = storage.Client()

        # Setup paths and download the metadata CSV to /tmp directory
        bucket = storage_client.bucket(bucket_name)
        local_dir = f"/tmp/{field_id}/{batch_id}/"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "image_metadata.csv")
        blob = bucket.blob(f"userdata/{field_id}/{batch_id}/image_metadata.csv")
        blob.download_to_filename(local_path)

        # Read the CSV and process data
        df = pd.read_csv(local_path)

        # List which contains tuples which contain the Latitude, Longitude, and Date for each row in the dataframe
        coordinates_and_dates = []

        for _, row in df.iterrows():
            lat = row['Latitude']
            lon = row['Longitude']
            date_created = row['Date']
            coordinates_and_dates.append((lat, lon, date_created))

        # Getting rid of duplicate tuples
        # Using an OrderedDict to remove duplicates while maintaining order.
        unique_coordinates_and_dates_ordered = list(OrderedDict.fromkeys(coordinates_and_dates))
        
        # Fetching MODIS values for each location and date
        results = [get_modis_values(lat, lon, date) for lat, lon, date in unique_coordinates_and_dates_ordered]
        
        # Save the remote sensing data to CSV in /tmp directory
        output_csv_path = os.path.join(local_dir, f"remote_sensing_data_{field_id}_{batch_id}.csv")
        write_to_csv(output_csv_path, results)

        # Upload the final CSV to the desired directory
        output_blob = bucket.blob(f"userdata/{field_id}/{batch_id}/remote_sensing_data.csv")
        output_blob.upload_from_filename(output_csv_path)

        # Cleanup /tmp/ directory
        os.remove(local_path)
        os.remove(output_csv_path)

        print(f"Remote sensing data processed and saved for batch {batch_id} in field {field_id}.")

        # Publish a new message to the remote-sensing-data-fetched topic
        publisher = pubsub_v1.PublisherClient()
        project_id = "tidy-nomad-415320"
        topic_name = "remote-sensing-data-fetched"
        topic_path = publisher.topic_path(project_id, topic_name)
        data = {"bucket": bucket_name, "field_id": field_id, "batch_id": batch_id}
        message = json.dumps(data).encode("utf-8")
        future = publisher.publish(topic_path, data=message)
        future.result()

        print("Published message to remote-sensing-data-fetched topic.")

    except Exception as e:
        print(f"Failed to process remote sensing data: {traceback.format_exc()}")

    # Safely remove all temporary files after processing and uploading CSV
    shutil.rmtree(local_dir, ignore_errors=True)