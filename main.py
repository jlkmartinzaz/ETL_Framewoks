import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

# Cargar variables de entorno
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Rutas
CSV_INPUT = "data/clean_lung_cancer.csv"
PARQUET_OUTPUT = "data/clean_lung_cancer.parquet"

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL Lung Cancer") \
        .getOrCreate()

def transform_data(spark, csv_path):
    # Leer CSV limpio
    df = spark.read.option("header", True).csv(csv_path)
    
    # Transformaciones simples: eliminar espacios y convertir tipos
    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))
    
    # Convertir columnas numéricas
    df = df.withColumn("age", col("age").cast("integer"))
    df = df.withColumn("pack_years", col("pack_years").cast("double"))
    
    return df

def save_backup(df):
    df.write.mode("overwrite").parquet(PARQUET_OUTPUT)
    df.write.mode("overwrite").option("header", True).csv(CSV_INPUT.replace(".csv", "_backup.csv"))
    print(f"✅ Datos guardados en {PARQUET_OUTPUT} y backup CSV")

def save_to_postgres(df):
    try:
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        # Convertir a pandas y guardar
        df.toPandas().to_sql("lung_cancer", engine, if_exists="replace", index=False)
        print("✅ Datos cargados en tabla SQL: lung_cancer")
    except Exception as e:
        print(f"❌ Error de conexión SQL: {e}")

def main():
    spark = create_spark_session()
    
    print("🔹 Transformando datos...")
    df = transform_data(spark, CSV_INPUT)
    
    print("🔹 Guardando backup...")
    save_backup(df)
    
    print("🔹 Cargando a PostgreSQL...")
    save_to_postgres(df)
    
    spark.stop()
    print("✅ ETL completo")

if __name__ == "__main__":
    main()
