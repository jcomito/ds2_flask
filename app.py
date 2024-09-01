import os
import gzip
import io
import logging
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# Directory where files will be saved
SAVE_DIRECTORY = '/srv/http/ds2.jasoncomito.com/ingested_data'

# Ensure the directory exists
os.makedirs(SAVE_DIRECTORY, exist_ok=True)

# Set up detailed logging
logging.basicConfig(level=logging.DEBUG)

@app.route('/ingest', methods=['POST'])
def ingest():
    # Log headers and content type for debugging
    logging.debug("Received request headers: %s", request.headers)
    logging.debug("Received content type: %s", request.content_type)
    logging.debug("Received content encoding: %s", request.content_encoding)

    try:
        # Handle gzip-encoded data
        if request.content_encoding == 'gzip':
            compressed_data = request.get_data()
            file_content = gzip.GzipFile(fileobj=io.BytesIO(compressed_data)).read().decode('utf-8')
        elif request.content_length > 0:
            # Handle uncompressed data
            file_content = request.data.decode('utf-8')
        else:
            return jsonify({"error": "No content in request"}), 400

        # Create a timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Generate a filename with the timestamp
        filename = f"ingested_data_{timestamp}.json"

        # Save the content to a file with the timestamped name
        file_path = os.path.join(SAVE_DIRECTORY, filename)
        with open(file_path, 'w') as f:
            f.write(file_content)

        logging.info("File %s received and processed successfully", filename)
        return jsonify({"message": f"File {filename} received and processed successfully"}), 200

    except Exception as e:
        logging.error("Error processing request: %s", str(e))
        return jsonify({"error": "Internal Server Error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0')
