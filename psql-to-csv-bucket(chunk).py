import psycopg2
import pandas as pd
from google.cloud import storage
import os
from io import StringIO

# Set Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/ibrahim_md/key.json"

# Database connection parameters
db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "ib1234",
    "host": "34.28.254.151",
    "port": "5432"
}

# GCS bucket parameters
bucket_name = "psql-gcs"  # Replace with your GCS bucket name
gcs_folder = "output_data_chunks"  # Folder in GCS to store chunks
chunk_size = 200000  # Number of rows per chunk

# Function to upload a chunk to GCS from memory
def upload_chunk_to_gcs(bucket_name, destination_blob_name, data):
    """
    Uploads a CSV data chunk to a GCS bucket from memory.

    :param bucket_name: Name of the GCS bucket.
    :param destination_blob_name: The name of the blob in GCS.
    :param data: CSV data as string to upload.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data, content_type='text/csv')
    print(f"Uploaded chunk to {bucket_name}/{destination_blob_name}")

# Connect to the PostgreSQL database and retrieve data in chunks
try:
    # Establish connection
    connection = psycopg2.connect(**db_config)
    print("Database connection established.")

    # Read data in chunks and upload each to GCS
    chunk_number = 1
    for chunk in pd.read_sql("SELECT * FROM yelp_db", connection, chunksize=chunk_size):
        # Convert chunk DataFrame to CSV format in memory
        csv_data = StringIO()
        chunk.to_csv(csv_data, index=False)
        csv_data.seek(0)  # Move cursor to start of file for reading

        # Define GCS path for each chunk
        gcs_blob_name = f"{gcs_folder}/output_data_chunk_{chunk_number}.csv"
        
        # Upload chunk to GCS
        upload_chunk_to_gcs(bucket_name, gcs_blob_name, csv_data.getvalue())
        print(f"Chunk {chunk_number} uploaded to GCS as {gcs_blob_name}")
        chunk_number += 1

except Exception as e:
    print(f"Error exporting data to GCS: {e}")

finally:
    # Close the database connection
    if connection:
        connection.close()
        print("Database connection closed.")