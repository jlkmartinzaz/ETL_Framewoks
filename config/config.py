import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()


# Rutas y archivos
RAW_CSV = os.getenv("RAW_CSV", "data/lung_cancer.csv")
CLEAN_CSV = os.getenv("CLEAN_CSV", "data/clean_lung_cancer.csv")
BACKUP_PARQUET = os.getenv("BACKUP_PARQUET", "data/clean_lung_cancer.parquet")
BACKUP_CSV = os.getenv("BACKUP_CSV", "data/clean_lung_cancer_backup.csv")


# PostgreSQL
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Construir URL de conexión SQLAlchemy
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# HDFS
HDFS_HOST = os.getenv("HDFS_HOST")  # nombre del servicio en docker-compose
HDFS_PORT = os.getenv("HDFS_PORT")
HDFS_DIR = os.getenv("HDFS_DIR")

HDFS_URL = f"hdfs://{HDFS_HOST}:{HDFS_PORT}{HDFS_DIR}"

# Función para crear sesión Spark
def get_spark_session(app_name: str = "ETL Lung Cancer") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
