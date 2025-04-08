#!/bin/sh

# Check that required environment variables are set
: "${MINIO_ROOT_USER:?Environment variable MINIO_ROOT_USER not set}"
: "${MINIO_ROOT_PASSWORD:?Environment variable MINIO_ROOT_PASSWORD not set}"

# Start MinIO server in the background
minio server /data --console-address ":9001" &

# Wait until MinIO is ready
echo "Waiting for MinIO to start..."
until curl -s http://localhost:9000/minio/health/live; do
  sleep 2
done
echo "MinIO is running!"

# Set MinIO alias using env variables
mc alias set myminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Ensure bucket is created
if mc ls myminio/fintech-data >/dev/null 2>&1; then
  echo "Bucket 'fintech-data' already exists."
else
  echo "Creating bucket 'fintech-data'..."
  mc mb myminio/fintech-data
  echo "Bucket 'fintech-data' created successfully."
fi

# Keep the container running
wait
