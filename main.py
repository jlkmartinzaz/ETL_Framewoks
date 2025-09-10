from config.config import CSV_INPUT, PARQUET_OUTPUT, CSV_BACKUP, HDFS_PATH, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
from extract.extractor import create_spark_session, extract_data
from transform.transformer import transform_data
from load.loader import save_backup, save_to_hdfs, save_to_postgres

def main():
    try:
        spark = create_spark_session()

        print("1. Extrayendo datos...")
        df_raw = extract_data(spark, CSV_INPUT)

        print("2. Transformando datos...")
        df_clean = transform_data(df_raw)

        print("3. Guardando backups...")
        save_backup(df_clean, PARQUET_OUTPUT, CSV_BACKUP)

        print("4. Guardando en HDFS...")
        save_to_hdfs(df_clean, HDFS_PATH)

        print("5. Cargando a PostgreSQL...")
        save_to_postgres(df_clean, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    except Exception as e:
        print(f"Error en ETL: {e}")

    finally:
        spark.stop()
        print("ETL completo")

if __name__ == "__main__":
    main()
