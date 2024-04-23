from pyspark.sql import SparkSession



spark = SparkSession.builder \
	.appName("CamAda Gold") \
	.getOrCreate()

# Yellow Silver
yellow_silver_path = "s3://767398038964-raw/silver/yellow_processed/"
# Lendo arquivos Yellow Silver
yellow_silver_df = spark.read.parquet(yellow_silver_path)
yellow_silver_df.createOrReplaceTempView("yellow_silver_view")

# Green Silver

green_silver_path = "s3://767398038964-raw/silver/green_processed/"
green_silver_df = spark.read.parquet(green_silver_path)
green_silver_df.createOrReplaceTempView("green_silver_view")

#Query da Gold
gold_df = spark.sql("""
    SELECT 
        vendor_id,
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        rate_code_id,
        pu_location_id,
        do_location_id,
        CASE WHEN passenger_count IS NULL THEN 1 ELSE passenger_count END AS passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        CASE WHEN improvement_surcharge IS NULL THEN 0 ELSE improvement_surcharge END AS improvement_surcharge,
        total_amount,
        CASE WHEN payment_type IS NULL THEN 5 ELSE payment_type END AS payment_type,
        NULL AS trip_type,
        'yellow' taxi_type,
        partition_date
    FROM yellow_silver_view
    UNION ALL
      SELECT 
        vendor_id,
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        rate_code_id,
        pu_location_id,
        do_location_id,
        CASE WHEN passenger_count IS NULL THEN 1 ELSE passenger_count END AS passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        CASE WHEN improvement_surcharge IS NULL THEN 0 ELSE improvement_surcharge END AS improvement_surcharge,
        total_amount,
        CASE WHEN payment_type IS NULL THEN 5 ELSE payment_type END AS payment_type,
        CASE WHEN trip_type IS NULL THEN 1 ELSE trip_type END AS trip_type,
        'green' taxi_type,
        partition_date
    FROM green_silver_view 
""")

path = "s3://767398038964-raw/gold/taxi/"

# Salvando o DataFrame em formato Parquet
gold_df.write.mode("overwrite").partitionBy("partition_date").parquet(path)