# config/config.py

class Config:
    APP_NAME = "LungCancerETL"
    MASTER = "local[*]"

    # Como pediste: dataset está en el raíz
    INPUT_PATH = "lung_cancer_dataset.csv"
    OUTPUT_PATH = "data/output/clean_data"

    INPUT_FORMAT = "csv"
    OUTPUT_FORMAT = "parquet"

    CSV_OPTIONS = {
        "header": True,
        "inferSchema": True
    }
