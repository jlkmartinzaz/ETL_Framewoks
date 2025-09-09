from pyspark.sql import SparkSession
from extract.extractor import extract_data
from transform.transformer import transform_data
from load.loader import save_to_parquet, save_to_csv

if __name__ == "__main__":
    # 1. Crear sesión de Spark
    spark = SparkSession.builder.appName("LungCancerETL").getOrCreate()

    # 2. Extraer datos (ajusta la ruta si tu CSV no está en la raíz)
    df_raw = extract_data(spark, "lung_cancer.csv")

    # 3. Transformar datos
    df_clean = transform_data(df_raw)

    # 4. Cargar datos (guardar en Parquet y CSV)
    save_to_parquet(df_clean, "data/clean_lung_cancer.parquet")
    save_to_csv(df_clean, "data/clean_lung_cancer.csv")

    spark.stop()
