from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/ingest', methods=['GET'])
def ingest():
    return jsonify({"message": "ingest on ds2.jasoncomito.com"})

if __name__ == '__main__':
    app.run(host='0.0.0.0')
