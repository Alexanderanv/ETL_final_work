#!/usr/bin/env python3

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, monotonically_increasing_id

def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-write-app") \
        .getOrCreate()

    # Чтение parquet-файла
    df = spark.read.parquet("s3a://etl-final-task2/destination_files/2025_06_19_online_retail_2009_2010.parquet").cache()
    
    # Добавляем последовательный ID для упорядочивания
    df_with_id = df.withColumn("row_id", monotonically_increasing_id())
    total_rows = df_with_id.count()
    batch_size = 1000
    current_offset = 0

    while current_offset < total_rows:
        # Выбираем следующую партию строк
        batch_df = df_with_id.filter(
            (col("row_id") >= current_offset) & 
            (col("row_id") < current_offset + batch_size)
        )
        
        # Подготавливаем данные для Kafka
        kafka_df = batch_df.select(
            to_json(struct([col(c) for c in df.columns])).alias("value")
        )

        # Отправляем в Kafka
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "KAFKA_HOST_FQDN:9091") \
            .option("topic", "online-retail") \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
            .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=USER "
                "password=PASSWORD"
                ";") \
                .save()

        current_offset += batch_size
        time.sleep(3)

    spark.stop()

if __name__ == "__main__":
    main()