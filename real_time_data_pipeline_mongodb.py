from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import pyspark.sql.functions as fn

import random

# Kafka Broker/Cluster Details
KAFKA_TOPIC_NAME_CONS = "commerce"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

# Cassandra Cluster Details
cassandra_connection_host = "localhost"
cassandra_connection_port = "9042"
cassandra_keyspace_name = "trans_ks"
cassandra_table_name = "trans_message_detail_tbl"

# MongoDB Cluster Details
mongodb_host_name = "localhost"
mongodb_port_no = "27017"
mongodb_user_name = "demouser"
mongodb_password = "demouser"
mongodb_database_name = "trans_db"

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def save_to_cassandra_table(current_df, epoc_id):
    print("Inside save_to_cassandra_table function")
    print("Printing epoc_id: ")
    print(epoc_id)

    current_df \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("spark.cassandra.connection.host", cassandra_connection_host) \
    .option("spark.cassandra.connection.port", cassandra_connection_port) \
    .option("keyspace", cassandra_keyspace_name) \
    .option("table", cassandra_table_name) \
    .save()
    print("Exit out of save_to_cassandra_table function")

def save_to_mongodb_collection(current_df, epoc_id, mongodb_collection_name):
    print("Inside save_to_mongodb_collection function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing mongodb_collection_name: " + mongodb_collection_name)

    spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name

    current_df.write.format("mongo") \
        .mode("append") \
        .option("uri", spark_mongodb_output_uri) \
        .option("database", mongodb_database_name) \
        .option("collection", mongodb_collection_name) \
        .save()

    print("Exit out of save_to_mongodb_collection function")

if __name__ == "__main__":
    print("Real-Time Data Pipeline Started ...")

    spark = SparkSession \
        .builder \
        .appName("Real-Time Data Pipeline") \
        .master("local[*]") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from transmessage
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    # Code Block 4 Starts Here
    transaction_detail_schema = StructType([
      StructField("results", ArrayType(StructType([
        StructField("user", StructType([
          StructField("gender", StringType()),
          StructField("name", StructType([
            StructField("title", StringType()),
            StructField("first", StringType()),
            StructField("last", StringType())
          ])),
          StructField("location", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip", IntegerType())
          ])),
          StructField("email", StringType()),
          StructField("username", StringType()),
          StructField("password", StringType()),
          StructField("salt", StringType()),
          StructField("md5", StringType()),
          StructField("sha1", StringType()),
          StructField("sha256", StringType()),
          StructField("registered", IntegerType()),
          StructField("dob", IntegerType()),
          StructField("phone", StringType()),
          StructField("cell", StringType()),
          StructField("PPS", StringType()),
          StructField("picture", StructType([
            StructField("large", StringType()),
            StructField("medium", StringType()),
            StructField("thumbnail", StringType())
          ]))
        ]))
      ]), True)),
      StructField("nationality", StringType()),
      StructField("seed", StringType()),
      StructField("version", StringType()),
      StructField("tran_detail", StructType([
        StructField("tran_card_type", ArrayType(StringType())),
        StructField("product_id", StringType()),
        StructField("tran_amount", DoubleType())
      ]))
    ])
    # Code Block 4 Ends Here

    transaction_detail_df_1 = transaction_detail_df.selectExpr("CAST(value AS STRING)")

    transaction_detail_df_2 = transaction_detail_df_1.select(from_json(col("value"), transaction_detail_schema).alias("message_detail"))

    transaction_detail_df_3 = transaction_detail_df_2.select("message_detail.*")

    print("Printing Schema of transaction_detail_df_3: ")
    transaction_detail_df_3.printSchema()

    transaction_detail_df_4 = transaction_detail_df_3.select(explode(col("results.user")).alias("user"),
                                                            col("nationality"),
                                                            col("seed"),
                                                            col("version"),
                                                            col("tran_detail.tran_card_type").alias("tran_card_type"),
                                                            col("tran_detail.product_id").alias("product_id"),
                                                            col("tran_detail.tran_amount").alias("tran_amount")
                                                            )

    transaction_detail_df_5 = transaction_detail_df_4.select(
      col("user.gender"),
      col("user.name.title"),
      col("user.name.first"),
      col("user.name.last"),
      col("user.location.street"),
      col("user.location.city"),
      col("user.location.state"),
      col("user.location.zip"),
      col("user.email"),
      col("user.username"),
      col("user.password"),
      col("user.salt"),
      col("user.md5"),
      col("user.sha1"),
      col("user.sha256"),
      col("user.registered"),
      col("user.dob"),
      col("user.phone"),
      col("user.cell"),
      col("user.PPS"),
      col("user.picture.large"),
      col("user.picture.medium"),
      col("user.picture.thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      col("tran_card_type"),
      col("product_id"),
      col("tran_amount")
    )

    def randomCardType(transaction_card_type_list):
        return random.choice(transaction_card_type_list)

    getRandomCardType = udf(lambda transaction_card_type_list: randomCardType(transaction_card_type_list), StringType())

    transaction_detail_df_6 = transaction_detail_df_5.select(
      col("gender"),
      col("title"),
      col("first").alias("first_name"),
      col("last").alias("last_name"),
      col("street"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      concat(col("username"), round(rand() * 1000, 0).cast(IntegerType())).alias("user_id"),
      col("password"),
      col("salt"),
      col("md5"),
      col("sha1"),
      col("sha256"),
      col("registered"),
      col("dob"),
      col("phone"),
      col("cell"),
      col("PPS"),
      col("large"),
      col("medium"),
      col("thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      getRandomCardType(col("tran_card_type")).alias("tran_card_type"),
      concat(col("product_id"), round(rand() * 100, 0).cast(IntegerType())).alias("product_id"),
      round(rand() * col("tran_amount"), 2).alias("tran_amount")
    )

    transaction_detail_df_7 = transaction_detail_df_6.withColumn("tran_date",
      from_unixtime(col("registered"), "yyyy-MM-dd HH:mm:ss"))

    # Write raw data into HDFS
    transaction_detail_df_7.writeStream \
      .trigger(processingTime='5 seconds') \
      .format("json") \
      .option("path", "hdfs://localhost:9000/tmp/data") \
      .option("checkpointLocation", "/home/enes/Applications/data") \
      .start()

    transaction_detail_df_8 = transaction_detail_df_7.select(
      col("user_id"),
      col("first_name"),
      col("last_name"),
      col("gender"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      col("nationality"),
      col("tran_card_type"),
      col("tran_date"),
      col("product_id"),
      col("tran_amount"))

    transaction_detail_df_8 \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .foreachBatch(save_to_cassandra_table) \
    .start()

    # Data Processing/Data Transformation
    transaction_detail_df_9 = transaction_detail_df_8.withColumn("tran_year", \
      year(to_timestamp(col("tran_date"), "yyyy")))

    year_wise_total_sales_count_df = transaction_detail_df_9.groupby('tran_year').agg(
        fn.count('tran_amount').alias('tran_year_count'))

    mongodb_collection_name = "year_wise_total_sales_count"

    year_wise_total_sales_count_df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .foreachBatch(lambda current_df, epoc_id: save_to_mongodb_collection(current_df, epoc_id, mongodb_collection_name)) \
    .start()

    country_wise_total_sales_count_df = transaction_detail_df_9.groupby('nationality').agg(
        fn.count('tran_amount').alias('tran_country_count'))

    mongodb_collection_name_1 = "country_wise_total_sales_count"

    country_wise_total_sales_count_df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .foreachBatch(lambda current_df, epoc_id: save_to_mongodb_collection(current_df, epoc_id, mongodb_collection_name_1)) \
    .start()

    card_type_wise_total_sales_count_df = transaction_detail_df_9.groupby('tran_card_type').agg(
        fn.count('tran_card_type').alias('tran_card_type_count'))

    card_type_wise_total_sales_df = transaction_detail_df_9.groupby('tran_card_type').agg(
        fn.sum('tran_card_type').alias('tran_card_type_total_sales'))

    year_country_wise_total_sales_df = transaction_detail_df_9.groupby("tran_year","nationality").agg(
        fn.sum('tran_amount').alias('tran_year_country_total_sales'))

    # Write result dataframe into console for debugging purpose
    trans_detail_write_stream = year_country_wise_total_sales_df \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    trans_detail_write_stream.awaitTermination()

    print("Real-Time Data Pipeline Completed.")
