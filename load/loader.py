from pyspark.sql.functions import col
from sqlalchemy import create_engine
import pandas as pd

def save_backup(df, parquet_path, csv_path):
    try:
        df.write.mode("overwrite").parquet(parquet_path)
        df.write.mode("overwrite").option("header", True).csv(csv_path)
        print(f"Backup guardado en {parquet_path} y {csv_path}")
    except Exception as e:
        print(f"Error guardando backup: {e}")
        raise

def save_to_hdfs(df, hdfs_path):
    try:
        df.write.mode("overwrite").parquet(hdfs_path)
        print(f"Datos guardados en HDFS: {hdfs_path}")
    except Exception as e:
        print(f"Error guardando en HDFS: {e}")

def save_to_postgres(df, db_user, db_pass, db_host, db_port, db_name):
    try:
        errors = df.filter(
            (col("age").isNull()) |
            (col("pack_years").isNull()) |
            (col("lung_cancer").isNull())
        ).count()
        if errors > 0:
            raise ValueError(f"{errors} filas con errores, abortando carga SQL")

        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
        df.toPandas().to_sql("lung_cancer", engine, if_exists="replace", index=False)
        
        print("Datos cargados en SQL: lung_cancer")
    except Exception as e:
        print(f"Error SQL: {e}")
