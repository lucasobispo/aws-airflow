from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Green Taxi Data Analysis") \
    .getOrCreate()

green_df = spark.read.parquet("s3://767398038964-raw/silver/green/")

green_df.createOrReplaceTempView("green_view")

result_tratado_df = spark.sql("""
    SELECT 
        VendorID AS vendor_id,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
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
        trip_type,
        CAST(lpep_pickup_datetime AS date) AS partition_date
    FROM green_view
""")

path = "s3://767398038964-raw/silver/green_processed/"


result_tratado_df.write.mode("overwrite").partitionBy("partition_date").parquet(path)