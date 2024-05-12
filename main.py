from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import os
import subprocess

# Extraction: Web scraping functions
def fetch_data_from_dawn():
    url = "https://www.dawn.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = [link['href'] for link in soup.find_all('a')]
    titles = [title.text.strip() for title in soup.find_all('h2')]
    descriptions = [desc.text.strip() for desc in soup.find_all('p', class_='story__excerpt')]
    return {"links": links, "titles": titles, "descriptions": descriptions}

def fetch_data_from_bbc():
    url = "https://www.bbc.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = [link['href'] for link in soup.find_all('a')]
    titles = [title.text.strip() for title in soup.find_all('h3')]
    descriptions = [desc.text.strip() for desc in soup.find_all('p')]
    return {"links": links, "titles": titles, "descriptions": descriptions}


def convert_to_json(ti):
    extracted_data_dawn = ti.xcom_pull(task_ids='extract_dawn')
    extracted_data_bbc = ti.xcom_pull(task_ids='extract_bbc')
    
    # Merge dictionaries
    extracted_data = {}
    extracted_data.update(extracted_data_dawn)
    extracted_data.update(extracted_data_bbc)
    
    transformed_data = json.dumps(extracted_data)
    ti.xcom_push(key='transformed_data', value=transformed_data)


# Loading: Save data to a file
def save_json_data(ti):
    data = ti.xcom_pull(key='transformed_data', task_ids='transform')
    with open('E:\Mlops\Airflow\data\combined_data.json', 'w') as file:
        file.write(data)
    
# DVC: Version and push the data
def version_and_push_data():
    data_dir = 'E:/Mlops/Airflow/data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Change to the data directory
    os.chdir(data_dir)
    subprocess.run(['dvc', 'add', 'combined_data.json'])
    subprocess.run(['git', 'add', '.'])
    subprocess.run(['git', 'commit', '-m', 'Update data'])
    subprocess.run(['dvc', 'push'])
    subprocess.run(['git', 'push'])

# DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

dag = DAG(
    'ans_mlops_dag',
    default_args=default_args,
    description='mlops',
    schedule_interval=timedelta(days=1),
    # catchup=False
)

# Define tasks
extract_dawn_task = PythonOperator(
    task_id='extract_dawn',
    python_callable=fetch_data_from_dawn,
    dag=dag,
)

extract_bbc_task = PythonOperator(
    task_id='extract_bbc',
    python_callable=fetch_data_from_bbc,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=convert_to_json,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=save_json_data,
    dag=dag,
)

version_task = PythonOperator(
    task_id='version_data',
    python_callable=version_and_push_data,
    dag=dag,
)

# Set task dependencies
# extract_dawn_task >> transform_task
# extract_bbc_task >> transform_task
extract_dawn_task >> extract_bbc_task >> transform_task >> load_task >> version_task
