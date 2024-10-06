from flask import Blueprint, jsonify, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required, current_user
from flask import current_app
from werkzeug.utils import secure_filename
from . import db
from .models import User, Field, Batch, Image
import os
from google.cloud import pubsub_v1
from .config import Config
from .file_utils import create_directory, save_file
import re
from datetime import datetime
import json
import base64
from .services import update_image_predictions_gcp, update_image_status_to_predicting
from google.cloud import storage
import logging
from datetime import timedelta

main = Blueprint('main', __name__, url_prefix='/main')

publisher = pubsub_v1.PublisherClient()
project = Config.PROJECT
bucket_name = Config.GCS_BUCKET_NAME

@main.route('/')
@login_required
def index():
    fields = Field.query.all()
    return render_template('index.html', fields=fields)

def allowed_file(filename):
    # Function to check for allowed file extensions
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

def parse_filename(filename):
    match = re.match(r"(\d+)_(\d+)_(\d+)_(\d+)_(\d{4}-\d{2}-\d{2})\.JPG", filename, re.IGNORECASE)
    if match:
        return {
            'field_code': match.group(1),
            'x_grid': int(match.group(2)),
            'y_grid': int(match.group(3)),
            'order': int(match.group(4)),
            'date_taken': datetime.strptime(match.group(5), "%Y-%m-%d").date()
        }
    return None

@main.route('/get_fields')
@login_required
def get_fields():
    fields = Field.query.all()
    fields_list = []
    for field in fields:
        fields_list.append({
            'field_id': field.id,
            'field_name': field.name,
            'user_name': field.user.name,
            'date_created': field.datetime.strftime('%Y-%m-%d %H:%M:%S')
        })
    return jsonify(fields_list)

@main.route('/get_batches/<int:field_id>')
@login_required
def get_batches(field_id):
    batches = Batch.query.filter_by(field_id=field_id).all()
    batches_list = []
    for batch in batches:
        batches_list.append({
            'batch_id': batch.id,
            'x_grid': batch.x_grid,
            'y_grid': batch.y_grid,
            'img_qty': batch.img_qty,
            'date_taken': batch.date_taken.strftime('%Y-%m-%d'),
            'user_name': batch.user.name,
            'date_uploaded': batch.datetime.strftime('%Y-%m-%d %H:%M:%S')
        })
    return jsonify(batches_list)

@main.route('/get_images/<int:batch_id>')
@login_required
def get_images(batch_id):
    images = Image.query.filter_by(batch_id=batch_id).all()
    images_list = []
    for image in images:
        images_list.append({
            'image_id': image.id,
            'filename': image.filename,
            'label': image.label,
            'healthy': image.healthy,
            'rice_blast': image.rice_blast,
            'brown_spot': image.brown_spot,
            'date_uploaded': image.datetime.strftime('%Y-%m-%d %H:%M:%S')
        })
    return jsonify(images_list)

@main.route('/add_field', methods=['POST'])
@login_required
def add_field():
    name = request.form.get('name')
    if name:
        try:
            new_field = Field(name=name, user_id=current_user.id)
            db.session.add(new_field)
            db.session.commit()

            field_directory = os.path.join(Config.UPLOAD_FOLDER, str(new_field.id))
            create_directory(field_directory)

            return jsonify({'success': True, 'message': 'Field added successfully.', 'newFieldId': new_field.id}), 200
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f'Error adding field: {e}')
            return jsonify({'success': False, 'message': 'Error adding field.'}), 500
    else:
        return jsonify({'success': False, 'message': 'Field name is required.'}), 400

@main.route('/upload_batch', methods=['POST'])
@login_required
def upload_batch():
    field_id = request.form.get('field_id')
    files = request.files.getlist('images')

    if not files:
        return jsonify({'success': False, 'message': 'No files provided.'}), 400
    
    metadata = parse_filename(files[0].filename)
    if not metadata:
        return jsonify({'success': False, 'message': 'Invalid filename format.'}), 400

    try:
        new_batch = Batch(user_id=current_user.id, field_id=field_id, 
                          x_grid=metadata['x_grid'], y_grid=metadata['y_grid'], 
                          img_qty=len(files), date_taken=metadata['date_taken'])
        db.session.add(new_batch)
        db.session.flush()

        # Construct the batch folder path using field_id and new_batch.id
        batch_folder = os.path.join(Config.UPLOAD_FOLDER, str(field_id), str(new_batch.id))
        create_directory(batch_folder)

        for file in files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(batch_folder, filename)
                save_file(file, batch_folder, filename)

                image_metadata = parse_filename(filename)
                
                new_image = Image(filename=filename, path=file_path, label='no',
                                  batch_id=new_batch.id, order=image_metadata['order'],
                                  date_taken=image_metadata['date_taken'])
                db.session.add(new_image)

        db.session.commit()
        if Config.ENV_MODE == 'development':
            extract_metadata(field_id, new_batch.id)
        else:            
            # Publish a message to the topic metadata-extraction-trigger
            topic_name = "metadata-extraction-trigger"
            topic_path = publisher.topic_path(project, topic_name)
            data = {"bucket": bucket_name, "field_id": str(field_id), "batch_id": str(new_batch.id)}
            message = json.dumps(data).encode("utf-8")
            future = publisher.publish(topic_path, data=message)
            future.result()

        return jsonify({
            'success': True,
            'message': 'Batch and images uploaded successfully.',
            'newBatch': {
                'batch_id': new_batch.id,
                'field_id': field_id,
            }
        }), 200
    except Exception as e:
        db.session.rollback()
        logging.error(f'Error uploading batch: {e}', exc_info=True)
        return jsonify({'success': False, 'message': str(e)}), 500
    
@main.route('/upload_complete', methods=['POST'])
@login_required
def upload_complete():
    data = request.get_json()
    field_id = data.get('field_id')
    batch_id = data.get('batch_id')

    try:
        if Config.ENV_MODE == 'development':
            extract_metadata(field_id, batch_id)
            message = 'Metadata extracted successfully.'
        else:
            # Publish a message to the topic metadata-extraction-trigger
            topic_name = "metadata-extraction-trigger"
            topic_path = publisher.topic_path(Config.PROJECT, topic_name)
            data = {"bucket": Config.GCS_BUCKET_NAME, "field_id": str(field_id), "batch_id": str(batch_id)}
            message_data = json.dumps(data).encode("utf-8")
            future = publisher.publish(topic_path, data=message_data)
            future.result()
            message = 'Images uploaded. Predicting...'

        return jsonify({'success': True, 'message': message}), 200
    except Exception as e:
        logging.error(f'Error processing post-upload actions: {e}', exc_info=True)
        return jsonify({'success': False, 'message': str(e)}), 500

    
@main.route('/update_predictions', methods=['POST'])
def update_predictions():
    # Extract the message data from the request
    message = request.get_json()
    data = message['message']['data']
    decoded_data = json.loads(base64.b64decode(data).decode('utf-8'))
    print("Decoded data:", decoded_data)

    field_id = decoded_data["field_id"]
    batch_id = decoded_data["batch_id"]
    
    # Call the function to update image predictions
    success, message = update_image_predictions_gcp(field_id, batch_id)
    
    if success:
        return jsonify({'status': 'success', 'message': message}), 200
    else:
        return jsonify({'status': 'error', 'message': message}), 500

@main.route('/set_images_to_predicting', methods=['POST'])
def set_images_to_predicting():
    # Extract the message data from the request
    message = request.get_json()
    data = message['message']['data']
    decoded_data = json.loads(base64.b64decode(data).decode('utf-8'))
    print("Decoded data:", decoded_data)

    batch_id = decoded_data["batch_id"]
    
    # Call the function to update image status
    success, message = update_image_status_to_predicting(batch_id)
    
    if success:
        return jsonify({'status': 'success', 'message': message}), 200
    else:
        return jsonify({'status': 'error', 'message': message}), 500

    
@main.route('/get_unique_batch_dates/<int:field_id>', methods=['GET'])
@login_required
def get_unique_batch_dates(field_id):
    batches = Batch.query.filter_by(field_id=field_id).order_by(Batch.date_taken).all()
    unique_dates = sorted(list(set(batch.date_taken for batch in batches)))
    return jsonify([date.strftime('%Y-%m-%d') for date in unique_dates])

@main.route('/get_images_by_date', methods=['GET'])
@login_required
def get_images_by_date():
    field_id = request.args.get('field_id', type=int)
    date = request.args.get('date')

    if not date:
        return jsonify({'error': 'Missing date parameter'}), 400
    try:
        date_parsed = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400
    
    batches = Batch.query.filter_by(field_id=field_id, date_taken=date_parsed).all()
    if not batches:
        return jsonify({'error': 'No batches found for the given field_id and date'}), 404

    images_data = []
    xGrid = batches[0].x_grid
    yGrid = batches[0].y_grid

    # Iterate through all batches to collect images
    for batch in batches:
        images = Image.query.filter_by(batch_id=batch.id).all()
        for image in images:
            images_data.append({
                'image_id': image.id,
                'filename': image.filename,
                'label': image.label,
                'order': image.order,
            })

    return jsonify({
        'images': images_data,
        'xGrid': xGrid,
        'yGrid': yGrid
    })

@main.route('/check_batch_update/<int:batch_id>')
def check_batch_update(batch_id):
    # Retrieve the current batch, if exists
    batch = Batch.query.get(batch_id)
    if batch:
        # Check if there are any images within the batch with the specified labels
        if is_batch_updated(batch_id):
            return jsonify(updated=True, xGrid=batch.x_grid, yGrid=batch.y_grid)
        else:
            return jsonify(updated=False)
    else:
        return jsonify(updated=False, error="Batch not found.")

def is_batch_updated(batch_id):
    labels_of_interest = ['predicting', 'Healthy', 'Brown Spot', 'Rice Blast']
    images = Image.query.filter(Image.batch_id == batch_id, Image.label.in_(labels_of_interest)).all()
    return len(images) > 0