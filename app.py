from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from celery import Celery
from PIL import Image
import requests
from io import BytesIO
import os
import uuid
from dotenv import load_dotenv
import csv
import logging
from flask_cors import CORS
import json
import requests
import time
from sqlalchemy.exc import StatementError
import pandas as pd

# Initialize the app and configure CORS
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["*"], "allow_headers": ["Content-Type", "Authorization"], "supports_credentials": True, "methods": ["GET", "POST", "OPTIONS"]}})

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Flask configuration from environment variables
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['CELERY_BROKER_URL'] = os.getenv('CELERY_BROKER_URL')
app.config['CELERY_RESULT_BACKEND'] = os.getenv('CELERY_RESULT_BACKEND')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB limit
app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_FOLDER', 'output_images')
app.config['WEBHOOK_URL'] = os.getenv('WEBHOOK_URL')  # Webhook URL for callbacks

# Initialize the database
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])
    logging.info(f"Created upload folder at {app.config['UPLOAD_FOLDER']}")

db = SQLAlchemy(app)

# Celery configuration
def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['CELERY_BROKER_URL'],
        backend=app.config['CELERY_RESULT_BACKEND']
    )
    celery.conf.update(app.config)
    return celery

celery = make_celery(app)

# Model definition
class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    product_name = db.Column(db.String(255), nullable=False)
    input_image_urls = db.Column(db.ARRAY(db.String), nullable=False)
    output_image_urls = db.Column(db.ARRAY(db.String))
    status = db.Column(db.String(50), default='pending')
    request_id = db.Column(db.String(36), unique=True, default=lambda: str(uuid.uuid4()))

# Initialize the database
with app.app_context():
    db.create_all()
    logging.info("Database initialized")

@celery.task(name='app.process_images')
def process_images(product_id):
    with app.app_context():
        retry_attempts = 3
        for attempt in range(retry_attempts):
            try:
                product = Product.query.filter(Product.id == product_id).first()
                if not product:
                    logging.error(f"Product with ID {product_id} not found")
                    return
                
                logging.info(f"Processing images for product ID: {product_id}")
                
                output_urls = []
                for url in product.input_image_urls:
                    url = url.replace(" ", "")  # Remove any spaces in the URL
                    logging.debug(f"Compressing image from URL: {url}")
                    compressed_url = compress_image(url)
                    if compressed_url:
                        output_urls.append(compressed_url)
                    else:
                        logging.error(f"Failed to compress image from {url}")
                
                product.output_image_urls = output_urls
                product.status = 'completed'
                db.session.flush()  # Synchronize the state with the database
                db.session.refresh(product)  # Refresh to avoid stale data
                db.session.commit()
                logging.info(f"Image processing completed for product ID: {product_id}")
                
                # Trigger webhook after processing is completed
                if app.config['WEBHOOK_URL']:
                    trigger_webhook(product)
                
                # Generate output CSV after processing is completed
                generate_output_csv(product)
                
                break
            except StatementError as e:
                logging.error(f"StatementError encountered on attempt {attempt + 1} for product ID {product_id}: {e}")
                db.session.rollback()
                time.sleep(2)  # Wait before retrying
            except Exception as e:
                logging.error(f"Unexpected error during processing for product ID {product_id}: {e}")
                db.session.rollback()
                return

def compress_image(image_url):
    try:
        response = requests.get(image_url)
        response.raise_for_status()  # Raise an error for bad status codes
        img = Image.open(BytesIO(response.content))
        img = img.resize((img.width // 2, img.height // 2))

        output_filename = f"{uuid.uuid4()}.jpg"
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        img.save(output_path, quality=50)  # Reduce JPEG quality to 50%

        logging.info(f"Image saved to {output_path}")
        return f'http://localhost:8000/output_images/{output_filename}'
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error while compressing image from URL {image_url}: {e}")
        return None
    except NotImplementedError as e:
        logging.error(f"Feature not implemented error for URL {image_url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error compressing image from URL {image_url}: {e}")
        return None

def trigger_webhook(product):
    try:
        payload = {
            'request_id': product.request_id,
            'product_name': product.product_name,
            'status': product.status,
            'output_image_urls': product.output_image_urls
        }
        headers = {'Content-Type': 'application/json'}
        response = requests.post(app.config['WEBHOOK_URL'], data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        logging.info(f"Webhook triggered successfully for product ID: {product.id}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to trigger webhook for product ID {product.id}: {e}")

def generate_output_csv(product):
    try:
        output_data = {
            'Serial Number': [product.id],
            'Product Name': [product.product_name],
            'Input Image Urls': [', '.join(product.input_image_urls)],
            'Output Image Urls': [', '.join(product.output_image_urls or [])]
        }
        df = pd.DataFrame(output_data)
        output_csv_path = os.path.join(app.config['UPLOAD_FOLDER'], f"output_{product.id}.csv")
        df.to_csv(output_csv_path, index=False)
        logging.info(f"Output CSV generated at {output_csv_path}")
    except Exception as e:
        logging.error(f"Failed to generate output CSV for product ID {product.id}: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST', 'OPTIONS'])
def upload_csv():
    if request.method == 'OPTIONS':
        # Send CORS headers for preflight check
        response = jsonify({'status': 'Preflight OK'})
        return response

    # Handle CSV file upload logic
    if 'file' not in request.files:
        logging.error("No file part in the request")
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    logging.info(f"Received file: {file.filename}")

    if file.filename == '':
        logging.error("No selected file")
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            # Logging the file size for debugging
            logging.debug(f"File size: {len(file.read())} bytes")
            file.seek(0)  # Reset file pointer to read again

            csv_data = file.read().decode('utf-8')
            csv_reader = csv.DictReader(csv_data.splitlines())
            logging.info("CSV file successfully read")

            products = []
            for row in csv_reader:
                input_image_urls = [url.strip() for url in row['Input Image Urls'].split(',')]
                product = Product(product_name=row['Product Name'], input_image_urls=input_image_urls)
                db.session.add(product)
                db.session.commit()
                logging.info(f"Product created with ID: {product.id}")

                process_images.delay(product.id)
                products.append({'request_id': product.request_id})

            return jsonify(products), 200

        except Exception as e:
            logging.error(f"Error processing CSV: {e}")
            db.session.rollback()
            return jsonify({'error': 'Failed to process CSV'}), 500
    else:
        logging.error("Invalid file type. Only CSVs are allowed")
        return jsonify({'error': 'Invalid file type. Only CSVs are allowed.'}), 400

@app.route('/status/<request_id>', methods=['GET'])
def check_status(request_id):
    logging.info(f"Checking status for request ID: {request_id}")
    product = Product.query.filter_by(request_id=request_id).first()
    if not product:
        logging.error(f"Request ID {request_id} not found")
        return jsonify({'message': 'Request ID not found'}), 404

    return jsonify({
        'product_name': product.product_name,
        'input_image_urls': product.input_image_urls,
        'output_image_urls': product.output_image_urls,
        'status': product.status
    }), 200

@app.route('/output_images/<filename>')
def serve_image(filename):
    logging.info(f"Serving image: {filename}")
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(debug=True, port=8000)