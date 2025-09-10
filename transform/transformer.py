from pyspark.sql.functions import col, trim

def transform_data(df):
    try:
        for c in df.columns:
            df = df.withColumn(c, trim(col(c)))
        df = df.withColumn("age", col("age").cast("integer"))
        df = df.withColumn("pack_years", col("pack_years").cast("double"))
        return df
    except Exception as e:
        print(f"Error transformando datos: {e}")
        raise
