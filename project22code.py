from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("TransactionProcessing").getOrCreate()

# Load CSV files from Cloud Storage
df = spark.read.option("header", True).csv("gs://bank-transactions-data/data/*.csv")

# Data Cleaning: Remove null and blank rows
df_cleaned = df.na.drop()

# Save cleaned data back to Cloud Storage
df_cleaned.coalesce(1).write.csv("gs://bank-transactions-data/cleaned_transactions.csv", header=True)

failed_txns = df_cleaned.filter(col("status") == "Failed")
failed_txns.coalesce(1).write.csv("gs://bank-transactions-data/failed_transactions.csv", header=True)

