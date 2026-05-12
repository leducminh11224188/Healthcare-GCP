from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
                    .appName("CPT Codes Ingestion") \
                    .getOrCreate()
                    
# configure variables
BUCKET_NAME = "healthcare-bucket-minhld"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"

# BigQuery Configuration
BQ_PROJECT = "healthcare-496102"
BQ_TABLE = f"{BQ_PROJECT}.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from cpt
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)
cptcodes_df.show()

# replace spaces with underscore
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# write to bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())