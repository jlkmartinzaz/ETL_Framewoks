# load/loader.py
import os
from pyspark.sql import DataFrame
from sqlalchemy import create_engine
from pyspark.sql.functions import col
from config.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, BACKUP_PARQUET, BACKUP_CSV

def save_backup(df: DataFrame, parquet_path=BACKUP_PARQUET, csv_path=BACKUP_CSV):
    try:
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        df.write.mode("overwrite").parquet(parquet_path)
        df.write.mode("overwrite").option("header", True).csv(csv_path)
        print(f"Backup guardado en {parquet_path} y {csv_path}")
    except Exception as e:
        print(f"Error guardando backup: {e}")


def save_to_postgres(df: DataFrame):
    try:
        errors = df.filter(
            (col("age").isNull()) |
            (col("pack_years").isNull()) |
            (col("lung_cancer").isNull())
        ).count()
        if errors > 0:
            raise ValueError(f"{errors} filas con errores, abortando carga SQL")

        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        df.toPandas().to_sql("lung_cancer", engine, if_exists="replace", index=False)
        print("Datos cargados en PostgreSQL: lung_cancer")
    except Exception as e:
        print(f"Error SQL: {e}")