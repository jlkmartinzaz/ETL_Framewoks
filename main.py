# main.py
from pyspark.sql import SparkSession
from extract.extractor import extract_data
from transform.transformer import transform_data
from load.loader import save_backup, save_to_postgres
from config.config import get_spark_session, CLEAN_CSV

def main():
    spark = None
    try:
        # Crear sesión Spark
        spark = get_spark_session()

        #Extracción
        print("1. Extrayendo datos...")
        df_raw = extract_data(spark, CLEAN_CSV)
        print(f"Datos extraídos: {df_raw.count()} filas, {len(df_raw.columns)} columnas")

        #Transformación
        print("2. Transformando datos...")
        df_clean = transform_data(df_raw)
        print(f"Datos transformados: {df_clean.count()} filas, {len(df_clean.columns)} columnas")

        #Guardar backup local
        print("3. Guardando backup local...")
        save_backup(df_clean)

        #Guardar en PostgreSQL
        print("4. Cargando a PostgreSQL...")
        save_to_postgres(df_clean)

        print("ETL completo")

    except Exception as e:
        print(f"Error en ETL: {e}")
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()