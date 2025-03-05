# Fintech Fraud Detection Data Pipeline

## Overview
This project implements a real-time fraud detection system using Apache Kafka, Apache Spark, and Machine Learning. The pipeline ingests transaction data, detects fraudulent activities using an Isolation Forest model, and stores the results in MinIO.

## Architecture
The system consists of the following components:

- **Kafka Producer**: Generates synthetic transaction data and publishes it to a Kafka topic.
- **Kafka Consumer (Spark Streaming)**: Reads transaction data from Kafka, applies a trained Isolation Forest model for fraud detection, and stores flagged transactions in MinIO.
- **Machine Learning Model**: An Isolation Forest model trained on synthetic transaction data to detect anomalies.
- **MinIO**: Object storage for storing processed fraudulent transactions.
- **PostgreSQL**: Database for potential future use (e.g., storing transaction logs).

## Project Structure
```
├── scripts
│   ├── kafka_transaction_producer.py  # Generates and sends transactions to Kafka
│   ├── spark_streaming_consumer.py    # Consumes transactions from Kafka, applies fraud detection
│   ├── train_isolation_forest.py      # Trains and saves the Isolation Forest model
│   ├── isolation_forest_model.pkl     # Trained machine learning model
│
├── docker-compose.yml                 # Orchestrates services (Kafka, Spark, MinIO, PostgreSQL)
├── requirements.txt                    # Python dependencies
├── minio-entrypoint.sh                 # Initialization script for MinIO
└── README.md                           # Project documentation
```

## Setup Instructions

### Prerequisites
- Docker & Docker Compose installed
- Python 3.8+

### Installation
1. Clone the repository:
   ```sh
   git clone <repository-url>
   cd fintech-data-pipeline
   ```

2. Build and start the services:
   ```sh
   docker-compose up --build
   ```

3. Check if Kafka is running:
   ```sh
   docker ps | grep kafka
   ```

### Running the Components
#### Start the Kafka Producer
The producer generates and sends transaction data to Kafka.
```sh
python scripts/kafka_transaction_producer.py
```

#### Train the Isolation Forest Model (if needed)
```sh
python scripts/train_isolation_forest.py
```

#### Start the Spark Streaming Consumer
The consumer reads transactions from Kafka, applies the fraud detection model, and saves flagged transactions to MinIO.
```sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 scripts/spark_streaming_consumer.py
```

## Technologies Used
- **Apache Kafka**: Message broker for real-time transaction streaming.
- **Apache Spark (Structured Streaming)**: Consumes transaction data and applies fraud detection.
- **Scikit-learn (Isolation Forest)**: Machine learning model for anomaly detection.
- **MinIO (S3-Compatible Storage)**: Stores fraudulent transactions.
- **PostgreSQL**: Future storage for transaction logs.

## Future Improvements
- Store transaction logs in PostgreSQL.
- Enhance the fraud detection model with additional features.
- Implement alerting for fraudulent transactions.

## License
This project is open-source under the MIT License.