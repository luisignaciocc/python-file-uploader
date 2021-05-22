from celery import Celery
from dotenv import load_dotenv
from os import getenv
from process import UploadBudget

load_dotenv()

app = Celery('tasks', backend=getenv("CELERY_BACKEND_URL"), broker=getenv("CELERY_BROKER_URL"))

@app.task
def processDataCelery(filepath):
    UploadBudget(filepath).execute()