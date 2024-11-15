import os
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

# Initialize Google Cloud Clients
storage_client = storage.Client()
bq_client = bigquery.Client(project = 'local-bliss-437207-f0')

# Define the Cloud Storage bucket and BigQuery dataset
BUCKET_NAME = 'psql-gcs'
DATASET_NAME = 'psql_bq'
TABLE_NAME = 'cfunc-bq'

def load_csv_to_bigquery(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.
    This function loads a CSV file into BigQuery.
    """
    # Get the Cloud Storage file details
    file = event
    bucket_name = file['bucket']
    file_name = file['name']

    # Check if the file is in the expected bucket
    if bucket_name != BUCKET_NAME:
        print(f"File uploaded to an unexpected bucket: {bucket_name}. Expected {BUCKET_NAME}.")
        return

    print(f"File {file_name} uploaded to bucket {bucket_name}. Loading to BigQuery...")

    # Define BigQuery load job configuration
    uri = f'gs://{bucket_name}/{file_name}'
   
    # Configure the BigQuery load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row if present in the CSV
        autodetect=True,  # Automatically detect schema
    )
   
    # Define the destination table
    table_ref = bq_client.dataset(DATASET_NAME).table(TABLE_NAME)

   
        # Start the load job
    load_job = bq_client.load_table_from_uri(
        uri, table_ref, job_config=job_config
        )
       
        # Wait for the load job to complete
    load_job.result()
    print(f"File {file_name} successfully loaded into BigQuery table {TABLE_NAME}.")