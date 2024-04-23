from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Yellow Taxi Data Analysis") \
    .getOrCreate()

yellow_path = "s3://767398038964-raw/silver/yellow/"

yellow_df = spark.read.parquet(yellow_path)

yellow_df.createOrReplaceTempView("yellow_view")

yellow_tratado_df = spark.sql("""
    SELECT 
        VendorID AS vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        CASE WHEN RatecodeID is NULL THEN 1 ELSE RatecodeID END AS rate_code_id,
        PULocationID AS pu_location_id,
        DOLocationID AS do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        CAST(tpep_pickup_datetime AS date) AS partition_date
    FROM yellow_view
    WHERE tpep_pickup_datetime IS NOT NULL
""")

path = "s3://767398038964-raw/silver/yellow_processed/"

# Salvando o DataFrame em formato Parquet
yellow_tratado_df.write.mode("overwrite").partitionBy("partition_date").parquet(path)
