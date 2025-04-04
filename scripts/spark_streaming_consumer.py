from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import joblib
import os
import numpy as np

# Load Isolation Forest Model
model = joblib.load("/scripts/isolation_forest_model.pkl")

# Define fraud detection UDF
def detect_fraud(amount, user_id):
    transaction = np.array([[amount, user_id]])
    prediction = model.predict(transaction)
    return "FRAUD" if prediction[0] == -1 else "LEGIT"

fraud_udf = udf(detect_fraud, StringType())

# Load environment variables for MinIO
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# Kafka & S3 Settings
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "transactions_stream"
MINIO_PATH = "s3a://fintech-data/fraudulent_transactions_ml/"
CHECKPOINT_PATH = "s3a://fintech-data/checkpoints/"

# Define schema
schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("user_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("merchant", StringType()) \
    .add("location", StringType()) \
    .add("payment_method", StringType()) \
    .add("status", StringType())

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkMLFraudDetection") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false")\
    .load()

# Deserialize JSON data
transaction_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Apply fraud detection
fraudulent_transactions = transaction_stream \
    .withColumn("fraud_prediction", fraud_udf(col("amount"), col("user_id"))) \
    .filter(col("fraud_prediction") == "FRAUD")

# Try writing to MinIO
try:
    query = fraudulent_transactions.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", MINIO_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

except Exception as e:
    print(f"ERROR: {e}")
    spark.stop()
