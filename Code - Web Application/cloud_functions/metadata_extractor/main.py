import os
from google.cloud import storage, pubsub_v1
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
import pandas as pd
from datetime import datetime
import traceback
import shutil
import base64
import json

def get_exif_data(image_path):
    """Extracting EXIF data from an image."""
    image = Image.open(image_path)
    exif_data = {}
    if hasattr(image, '_getexif'):
        exif_info = image._getexif()
        if exif_info:
            for tag, value in exif_info.items():
                decoded = TAGS.get(tag, tag)
                exif_data[decoded] = value
    return exif_data

def get_gps_info(exif_data):
    """Extracting the GPSInfo dict from EXIF data."""
    for key, val in exif_data.items():
        if key == 'GPSInfo':
            gps_info = {}
            for t in val:
                sub_decoded = GPSTAGS.get(t, t)
                gps_info[sub_decoded] = val[t]
            return gps_info
    return None

def gps_info_to_decimal(gps_info):
    """Converting GPSInfo to decimal degrees for latitude and longitude."""
    def convert_to_degrees(value):
        """Converts GPS coordinates to decimal degrees."""
        d, m, s = value
        return d + (m / 60.0) + (s / 3600.0)

    if gps_info:
        lat = gps_info.get('GPSLatitude')
        lat_ref = gps_info.get('GPSLatitudeRef')
        lon = gps_info.get('GPSLongitude')
        lon_ref = gps_info.get('GPSLongitudeRef')

        if lat and lat_ref and lon and lon_ref:
            lat_decimal = convert_to_degrees(lat)
            lon_decimal = convert_to_degrees(lon)

            if lat_ref == 'S':
                lat_decimal = -lat_decimal
            if lon_ref == 'W':
                lon_decimal = -lon_decimal

            return lat_decimal, lon_decimal
    return None, None


def extract_date_time(exif_data):
    """Extracting the DateTime from EXIF data if available."""
    if 'DateTime' in exif_data:
        return exif_data['DateTime']
    return None

def metadata_extractor(event, context):
    """Triggered by a message from Pub/Sub indicating batch upload completion."""
    print("Raw event data:", event['data'])
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print("Decoded message:", pubsub_message)
    message_data = json.loads(pubsub_message)
    print("Message data:", message_data)

    bucket_name = message_data["bucket"]
    field_id = message_data["field_id"]
    batch_id = message_data["batch_id"]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    prefix = f'userdata/{field_id}/{batch_id}/'
    blobs = bucket.list_blobs(prefix=prefix)
    data_rows = []

    for blob in blobs:
        if blob.name.lower().endswith(('.jpg', '.jpeg')):
            image_path = '/tmp/' + os.path.basename(blob.name)
            blob.download_to_filename(image_path)

            try:
                exif_data = get_exif_data(image_path)
                gps_info = get_gps_info(exif_data)
                latitude, longitude = gps_info_to_decimal(gps_info) if gps_info else (None, None)
                date_time = extract_date_time(exif_data)

                # Prepare data for saving
                data_rows.append({
                    "Id": os.path.basename(blob.name),
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "Date and Time": date_time
                })
            except Exception as e:
                print(f"Error processing {blob.name}: {traceback.format_exc()}")
            finally:
                os.remove(image_path)

    # Convert all metadata to a DataFrame
    df = pd.DataFrame(data_rows)
    df['Date'] = pd.to_datetime(df['Date and Time'], format='%Y:%m:%d %H:%M:%S').dt.date.astype(str)
    csv_path = f'/tmp/{field_id}_{batch_id}_metadata.csv'
    df.to_csv(csv_path, index=False)
    metadata_blob = bucket.blob(f'userdata/{field_id}/{batch_id}/image_metadata.csv')
    metadata_blob.upload_from_filename(csv_path)

    # Publish message to Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    project_id = "tidy-nomad-415320"
    topic_name = "metadata-extracted"
    topic_path = publisher.topic_path(project_id, topic_name)
    data = {"bucket": bucket_name, "field_id": field_id, "batch_id": batch_id}
    message = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data=message)
    future.result()

    # Safely remove all temporary files after processing and uploading CSV
    shutil.rmtree('/tmp/', ignore_errors=True)