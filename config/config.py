import os
from dotenv import load_dotenv

load_dotenv()


# DB
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Rutas
CSV_INPUT = "data/clean_lung_cancer.csv"  
PARQUET_OUTPUT = "data/clean_lung_cancer.parquet"
CSV_BACKUP = "data/clean_lung_cancer_backup.csv"
HDFS_PATH = "hdfs://localhost:9000/lung_cancer"  
