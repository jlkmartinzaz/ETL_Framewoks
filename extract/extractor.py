from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from config.config import RAW_CSV

def get_spark_session(app_name="ETL_LungCancer") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def extract_data(spark: SparkSession, file_path: str = "data/clean_lung_cancer.csv") -> DataFrame:
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
        print(f"Datos extraídos: {df.count()} filas, {len(df.columns)} columnas")
        return df
    except Exception as e:
        print(f"Error extrayendo datos: {e}")
        raise
