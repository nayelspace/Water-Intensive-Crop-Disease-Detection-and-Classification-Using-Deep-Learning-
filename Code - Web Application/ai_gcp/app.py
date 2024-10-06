from flask import Flask, request, jsonify
import os
import pandas as pd
import numpy as np
import joblib
import tensorflow as tf
from tensorflow.keras.applications.densenet import preprocess_input as preprocess_input_densenet
from tensorflow.keras.models import load_model
from google.cloud import storage, pubsub_v1
import json
import shutil

app = Flask(__name__)

# Function to run the model with field_id and batch_id
def run_hybrid_model(field_id, batch_id, bucket_name):
    
    gcs_base_path = f"gs://{bucket_name}"
    
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Set up directories
    model_dir = "/tmp/model_artifacts"
    os.makedirs(model_dir, exist_ok=True)
    local_tmp_dir = f"/tmp/userdata/{field_id}/{batch_id}"  # Local directory for temporary files
    os.makedirs(local_tmp_dir, exist_ok=True)
    
    # Downloading model artifacts from GCS
    blob = bucket.blob("model_artifacts/label_encoder_v2_hybrid_model.joblib")
    blob.download_to_filename(os.path.join(model_dir, "label_encoder_v2_hybrid_model.joblib"))
    blob = bucket.blob("model_artifacts/scaler.joblib")
    blob.download_to_filename(os.path.join(model_dir, "scaler.joblib"))
    blob = bucket.blob("model_artifacts/Best_DenseNet121_Hybrid_Model.h5")
    blob.download_to_filename(os.path.join(model_dir, "Best_DenseNet121_Hybrid_Model.h5"))

    # Loading model components
    label_encoder = joblib.load(os.path.join(model_dir, 'label_encoder_v2_hybrid_model.joblib'))
    scaler = joblib.load(os.path.join(model_dir, 'scaler.joblib'))
    model = load_model(os.path.join(model_dir, "Best_DenseNet121_Hybrid_Model.h5"))

    # Construct paths
    image_folder = f"userdata/{field_id}/{batch_id}"
    combined_data_file = f"{image_folder}/combined_data.csv"
    local_csv_path = os.path.join(local_tmp_dir, "combined_data.csv")

    # Download the CSV file
    blob = bucket.blob(combined_data_file)
    blob.download_to_filename(local_csv_path)

    # Read numerical data
    numerical_df = pd.read_csv(local_csv_path)
    features_to_standardize = ['Avg Temp 14d', 'Avg Humidity 14d', 'Total Precipitation 14d', 'Avg Wind Speed 14d']
    all_numerical_features = ['Avg Temp 14d', 'Avg Humidity 14d', 'Total Precipitation 14d', 'Avg Wind Speed 14d', 'NDVI MODIS', 'NDVI - 1 MODIS', 'NDVI - 2 MODIS',
       'EVI MODIS', 'EVI - 1 MODIS', 'EVI - 2 MODIS', 'NDVI 1 Decrease',
       'NDVI 2 Decrease', 'EVI 1 Decrease', 'EVI 2 Decrease']
    
    numerical_df[features_to_standardize] = scaler.transform(numerical_df[features_to_standardize])

    # Function to preprocess images
    def preprocess_image_densenet121(image_path):
        image = tf.io.read_file(image_path)
        image = tf.image.decode_jpeg(image, channels=3)
        image = tf.image.resize_with_pad(image, 224, 224, antialias=True)
        image = preprocess_input_densenet(image)
        image = np.expand_dims(image, axis=0)
        return image

    # Preparing columns in the dataframe for predictions
    numerical_df['Class Confidence Levels'] = np.nan
    numerical_df['Class Prediction'] = np.nan

    # Adjust 'Id' column to include the full GCS path for images
    numerical_df['Id'] = numerical_df['Id'].apply(lambda x: f"{gcs_base_path}/{image_folder}/{x}")

    # Predicting and filling the dataframe with the predicted class and confidence levels
    for index, row in numerical_df.iterrows():
        img_array = preprocess_image_densenet121(row['Id'])
        num_data = row[all_numerical_features].to_numpy().reshape(1, -1)
        num_data = np.array(num_data, dtype=np.float32)

        # Generating class probability predictions
        prediction = model.predict([img_array, num_data])[0]

        # Formatting the predicted confidence levels for all classes
        confidences = {label_encoder.classes_[i]: round(float(prediction[i]), 4) for i in range(len(prediction))}

        # Sorting confidences so that the highest confidence is first
        sorted_confidences = dict(sorted(confidences.items(), key=lambda item: item[1], reverse=True))

        # Determining the predicted class
        predicted_class = max(sorted_confidences, key=sorted_confidences.get)

        # Updating the dataframe with the prediction and confidence levels
        numerical_df.at[index, 'Class Confidence Levels'] = str(sorted_confidences)
        numerical_df.at[index, 'Class Prediction'] = predicted_class

    # Selecting specific columns to save
    columns_to_save = ['Id', 'Latitude', 'Longitude', 'Date', 'Class Confidence Levels', 'Class Prediction']
    export_df = numerical_df[columns_to_save]

    # Save predictions locally
    local_csv_output = os.path.join(local_tmp_dir, "predictions_with_confidences.csv")
    local_json_output = os.path.join(local_tmp_dir, "predictions_with_confidences.json")
    export_df.to_csv(local_csv_output, index=False)
    export_df.to_json(local_json_output, orient='records')

    # Upload prediction files to GCS
    blob_csv = bucket.blob(f"{image_folder}/predictions_with_confidences.csv")
    blob_csv.upload_from_filename(local_csv_output)
    blob_json = bucket.blob(f"{image_folder}/predictions_with_confidences.json")
    blob_json.upload_from_filename(local_json_output)

    # Cleanup local temporary files
    shutil.rmtree(model_dir)
    shutil.rmtree(local_tmp_dir)

    # Publish a message to the topic predictions_made
    publisher = pubsub_v1.PublisherClient()
    project_id = "tidy-nomad-415320"
    topic_name = "predictions_made"
    topic_path = publisher.topic_path(project_id, topic_name)
    data = {"bucket": bucket_name, "field_id": field_id, "batch_id": batch_id}
    message = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data=message)
    future.result()

    return {'status': 'Success', 'message': 'Predictions generated and saved successfully.'}

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json(force=True)
    # Accessing the nested 'instances' array and extracting the first instance
    instance = data['instances'][0]
    field_id = instance['field_id']
    batch_id = instance['batch_id']
    bucket_name = instance['bucket']

    result = run_hybrid_model(field_id, batch_id, bucket_name)
    return jsonify(result)

# Health Check Route
@app.route('/health', methods=['GET'])
def health():
    """Health check route to ensure the application is up and running."""
    return jsonify({'status': 'UP'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)