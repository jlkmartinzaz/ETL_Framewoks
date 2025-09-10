from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform_data(df: DataFrame) -> DataFrame:
    """
    Limpieza básica de datos: quita duplicados y normaliza columnas.
    """
    df = df.dropDuplicates()

    # Normalizar strings (ejemplo: género)
    df = df.withColumn("gender", col("gender").cast("string"))

    return df


def validate_and_clean(df: DataFrame) -> DataFrame:
    """
    Validaciones adicionales: eliminar filas nulas y asegurar tipos.
    """
    df = df.dropna(how="any")
    df = df.withColumn("age", col("age").cast("int"))
    df = df.withColumn("pack_years", col("pack_years").cast("double"))

    print(f"✅ Datos validados: {df.count()} filas")
    return df
