import os
from flask import Flask, request, Response, jsonify, send_from_directory
from flask_cors import CORS
from multiprocessing import Process
from process import UploadBudget

app = Flask(__name__)
CORS(app)

@app.route('/download/sample', methods=['GET'])
def download_sample_file():
    return send_from_directory('static', "budget_base.xlsx", as_attachment=True)

@app.route('/upload', methods=['POST'])
def upload_budget():
    file = request.files.get('file', '')
    filepath = os.path.join("files/budget.xlsx")
    if not file:
        return Response("No file was sent from client", 422)
    file.save(filepath)

    worker_process = Process(
        name="upload_budget",
        target=processData,
        args=(filepath,),
        daemon=True
    )
    worker_process.start()

    return jsonify({
        "message": "File upload successfully, process started.",
        "pid": worker_process.pid
    }), 201

def processData(filepath):
    UploadBudget(filepath).execute()