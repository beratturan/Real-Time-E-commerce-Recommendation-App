from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql import SparkSession

"""
# Create an Elasticsearch client instance
es = Elasticsearch(["http://localhost:9200"])

# Define the mapping for the index
mapping = {
    "properties": {
        "title": {"type": "text"},
        "author": {"type": "keyword"},
        "description": {"type": "text"}
    }
}

# Create the index with the mapping
es.indices.create(index="views", body={"mappings": mapping})
"""



# Define the schema for the JSON data
schema = StructType([
    StructField("event", StringType(), True),
    StructField("messageid", StringType(), True),
    StructField("userid", StringType(), True),
    StructField("properties", StructType([
        StructField("productid", StringType(), True)
    ]), True),
    StructField("context", StructType([
        StructField("source", StringType(), True)
    ]), True),
    StructField("timestamp", StringType(), True)
])

  # Create a SparkSession
spark = SparkSession.builder.appName("Views").config("spark.jars","elasticsearch-hadoop-8.6.2.jar").getOrCreate()

  # Define the Kafka configuration
kafkaConfig = {
      "kafka.bootstrap.servers": "kafka:29092",
      "subscribe": "views",
      "startingOffsets": "earliest"
  }

  # Read the JSON data from Kafka as a stream
df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafkaConfig) \
    .load() \
    .selectExpr("CAST(value AS STRING)")

  # Parse the JSON data using the schema
parsed_df = df \
      .select(from_json(col("value"), schema).alias("data")) \
      .select("data.*")

selected_df = parsed_df \
    .select(
      col("event"),
      col("messageid"),
      col("userid"),
      col("properties.productid").alias("productid"),
      col("context.source").alias("source"),
      col("timestamp")
)

query = selected_df \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes","elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "views") \
    .option("checkpointLocation", "/tmp") \
    .start()

query.awaitTermination()