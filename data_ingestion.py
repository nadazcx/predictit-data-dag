import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator  
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient
import datetime  
import pendulum
from airflow.utils.dates import days_ago  #
dotenv_path = '/home/nada/data_project/.env'

load_dotenv(dotenv_path)
# Setting the start date using pendulum
start_date = pendulum.today('UTC').add(days=-2)


default_args = {
    'owner': 'nada',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


def json_scraper(url, file_name, container_name):
    print("Starting the JSON scraping process...") 
    try:
        # Scrape JSON data
        response = requests.get(url)
        response.raise_for_status()  
        json_data = response.json()
        print(f"Successfully scraped data from {url}.")  

        # Save to a local file
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)
        print(f"Data saved to {file_name}.")  


        blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_CONNECTION_STRING"))
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"predictit/{file_name}")
        
        with open(file_name, "rb") as data:
            blob_client.upload_blob(data)
        print(f"File {file_name} uploaded to Azure Blob Storage in container '{container_name}'.")  # Confirm upload

    except Exception as e:
        print(f"An error occurred: {e}")  

# Defining the DAG
with DAG(
    'predictit_dag', 
    default_args=default_args, 
    description="A DAG to scrape PredictIt data and upload it to Azure Blob Storage.",
    schedule=datetime.timedelta(days=1), 
    start_date=start_date,
    catchup=False,
    tags=["sdg"],
) as dag:
    
    extract_predictit = PythonOperator(
        task_id='extract_predictit',
        python_callable=json_scraper,
        op_kwargs={
            'url': "https://www.predictit.org/api/marketdata/all/",
            'file_name': 'predictit_markets.json',
            'container_name': "data-mbfr" 
        },
    )
    
    ready = EmptyOperator(task_id='ready')
    
    extract_predictit >> ready

