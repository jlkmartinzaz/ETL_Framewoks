from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def transform_data(df: DataFrame) -> DataFrame:
    try:
        # Limpieza básica: rellenar valores nulos
        df_clean = df.fillna({
            "age": 0,
            "pack_years": 0,
            "lung_cancer": 0
        })

        if "gender" in df_clean.columns:
            df_clean = df_clean.withColumn("gender", col("gender").cast("string"))
        return df_clean
    except Exception as e:
        print(f"Error transformando datos: {e}")
        raise