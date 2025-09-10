import os
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv()

# Config DB
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_DRIVER = os.getenv("DB_DRIVER", "postgresql+psycopg2")

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Config Hadoop
HDFS_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_URL = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"
