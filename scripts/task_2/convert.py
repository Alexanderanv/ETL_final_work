from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, to_timestamp
from pyspark.sql.types import IntegerType, StringType, BooleanType, DoubleType, StructType, StructField, TimestampType
from datetime import datetime

spark = SparkSession.builder.appName("export online sales to s3").getOrCreate()

current_date = datetime.today().strftime("%Y_%m_%d")

csv_filename = f"{current_date}_online_retail_2009_2010.csv"
source = f"s3a://etl-final-task2/source_files/{csv_filename}"
print(f"source path: {source}")

paruqet_filename = f"{current_date}_online_retail_2009_2010.parquet"
destination = f"s3a://etl-final-task2/destination_files/{paruqet_filename}"
print(f"destination path: {destination}")

schema = StructType([
    StructField("invoice", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True)
])

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(source)

df = df.withColumn(
    "invoice_date",
    coalesce(
        to_timestamp(col("invoice_date"), "M/d/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "M/d/yyyy HH:mm"),
        to_timestamp(col("invoice_date"), "MM/d/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "MM/d/yyyy HH:mm"),
        to_timestamp(col("invoice_date"), "M/dd/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "M/dd/yyyy HH:mm"),
        to_timestamp(col("invoice_date"), "MM/dd/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "MM/dd/yyyy HH:mm"),
        to_timestamp(col("invoice_date"), "d/M/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "d/M/yyyy HH:mm"),
        to_timestamp(col("invoice_date"), "dd/M/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "dd/M/yyyy HH:mm"),
        to_timestamp(col("invoice_date"), "d/MM/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "d/MM/yyyy HH:mm"),
        to_timestamp(col("invoice_date"), "dd/MM/yyyy H:mm"),
        to_timestamp(col("invoice_date"), "dd/MM/yyyy HH:mm")
    )
)

df.write.mode("overwrite").parquet(destination)

spark.stop()

print(f"Data successfully converted to {destination}")




