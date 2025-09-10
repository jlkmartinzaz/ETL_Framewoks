from pyspark.sql import SparkSession, DataFrame

def extract_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Lee un archivo CSV con Spark y lo devuelve como DataFrame.
    """
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )
    print(f"✅ Datos extraídos: {df.count()} filas, {len(df.columns)} columnas")
    return df
