import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from config.config import DB_URL, HDFS_URL

# ---- Guardar en local ----
def save_to_csv(df, path: str):
    df.write.mode("overwrite").option("header", True).csv(path)
    print(f"✅ Guardado CSV local: {path}")

def save_to_parquet(df, path: str):
    df.write.mode("overwrite").parquet(path)
    print(f"✅ Guardado Parquet local: {path}")

# ---- Guardar en HDFS ----
def save_to_hdfs(df, path: str, fmt: str = "parquet"):
    full_path = f"{HDFS_URL}/{path}"
    try:
        if fmt == "parquet":
            df.write.mode("overwrite").parquet(full_path)
        elif fmt == "csv":
            df.write.mode("overwrite").option("header", True).csv(full_path)
        else:
            raise ValueError("Formato no soportado")
        print(f"✅ Guardado en HDFS: {full_path}")
    except Exception as e:
        print(f"❌ Error guardando en HDFS: {e}")

# ---- Guardar en Base de Datos ----
def save_to_sql(df, table_name: str):
    try:
        pdf = df.toPandas()  # convertir a pandas (solo datasets manejables)
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            pdf.to_sql(table_name, con=conn, if_exists="replace", index=False)
        print(f"✅ Datos cargados en tabla SQL: {table_name}")
    except SQLAlchemyError as e:
        print(f"❌ Error de conexión SQL: {e}")
    except Exception as e:
        print(f"❌ Error inesperado en save_to_sql: {e}")
