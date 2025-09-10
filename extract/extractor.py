from pyspark.sql import SparkSession

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("ETL Lung Cancer") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"Error creando SparkSession: {e}")
        raise

def extract_data(spark, file_path):
    try:
        df = spark.read.option("header", True).csv(file_path)
        print(f"Datos extraídos: {df.count()} filas, {len(df.columns)} columnas")
        return df
    except Exception as e:
        print(f"Error extrayendo datos: {e}")
        raise
