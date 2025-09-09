from pyspark.sql import DataFrame

def save_to_parquet(df: DataFrame, output_path: str = "data/clean_lung_cancer.parquet") -> None:
    """
    Guarda un DataFrame de Spark en formato Parquet.

    Args:
        df (DataFrame): DataFrame de Spark a guardar.
        output_path (str): Ruta donde se guardará el archivo Parquet.
    """
    (
        df.write
        .mode("overwrite")  # sobrescribe si ya existe
        .parquet(output_path)
    )
    print(f"✅ Datos guardados en {output_path}")


def save_to_csv(df: DataFrame, output_path: str = "data/clean_lung_cancer.csv") -> None:
    """
    Guarda un DataFrame de Spark en formato CSV.

    Args:
        df (DataFrame): DataFrame de Spark a guardar.
        output_path (str): Ruta donde se guardará el archivo CSV.
    """
    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"✅ Datos guardados en {output_path}")
