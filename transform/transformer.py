from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lower

def transform_data(df: DataFrame) -> DataFrame:
    """
    Limpieza y transformaciones básicas del dataset de cáncer de pulmón.
    """
    # Normalizar género a minúsculas
    df = df.withColumn("gender", lower(col("gender")))

    # Reemplazar valores nulos en alcohol_consumption con 'unknown'
    df = df.withColumn(
        "alcohol_consumption",
        when(col("alcohol_consumption").isNull(), "unknown").otherwise(col("alcohol_consumption"))
    )

    # Convertir target 'lung_cancer' en binario 1 (Yes) / 0 (No)
    df = df.withColumn(
        "lung_cancer",
        when(col("lung_cancer") == "Yes", 1).otherwise(0)
    )

    return df


def validate_and_clean(df: DataFrame) -> DataFrame:
    """
    Validación y limpieza avanzada del dataset.
    """
    # Filtrar edades fuera de rango
    df = df.filter((col("age") > 0) & (col("age") <= 120))

    # Normalizar categorías de exposición (evitar valores inválidos)
    valid_radon = ["low", "medium", "high"]
    df = df.withColumn("radon_exposure", lower(col("radon_exposure")))
    df = df.filter(col("radon_exposure").isin(valid_radon))

    # Normalizar género (solo male/female)
    df = df.filter(col("gender").isin(["male", "female"]))

    # Limpiar consumo de alcohol (opciones válidas: none, moderate, heavy, unknown)
    valid_alcohol = ["none", "moderate", "heavy", "unknown"]
    df = df.withColumn("alcohol_consumption", lower(col("alcohol_consumption")))
    df = df.withColumn(
        "alcohol_consumption",
        when(col("alcohol_consumption").isin(valid_alcohol), col("alcohol_consumption")).otherwise("unknown")
    )

    return df
