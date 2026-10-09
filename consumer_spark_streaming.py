from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("SparkStreamingKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", IntegerType()),
    StructField("categoria", StringType()),
    StructField("monto", DoubleType()),
    StructField("timestamp", StringType())
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic-ventas") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_stats = df_parsed.groupBy("categoria").agg(
    count("id").alias("total_transacciones"),
    sum("monto").alias("monto_acumulado")
)

query = df_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

print("=== PROCESAMIENTO EN TIEMPO REAL INICIADO ===")
query.awaitTermination()
