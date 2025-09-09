from pyspark.sql import SparkSession, DataFrame

def extract_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Lee el dataset desde un archivo CSV usando PySpark.

    Args:
        spark (SparkSession): Sesión de Spark activa.
        file_path (str): Ruta del archivo CSV.

    Returns:
        DataFrame: DataFrame de Spark con los datos cargados.
    """
    df = (
        spark.read
        .option("header", "true")      # Usa la primera fila como cabecera
        .option("inferSchema", "true") # Detecta tipos de datos automáticamente
        .csv(file_path)
    )
    
    print(f"✅ Datos extraídos desde {file_path}, filas: {df.count()}, columnas: {len(df.columns)}")
    return df
