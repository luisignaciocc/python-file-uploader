from dotenv import load_dotenv
import os

load_dotenv()

mysql_conn = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "db": os.getenv("MYSQL_DB"),
    "user": os.getenv("MYSQL_USER"),
    "pass": os.getenv("MYSQL_PASS")
}