from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import tarfile
import os
import requests


def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print("File downloaded successfully: {}".format(filename))
    else:
        print("Failed to download file: {}".format(response.status_code))

# URL of the file to download
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"

# Filename to save the downloaded file
filename = "tolldata.tgz"



# Define the default arguments for the DAG
default_args = {
    'owner': 'andre',
    'start_date': datetime.today(),
    'email': 'andre@example.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}




# Define the DAG with the specified arguments
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)


# Function to unzip data
def unzip_data():
    tgz_file_path = "/home/project/airflow/dags/finalassignment/staging/tolldata.tgz"
    # Destination directory to unzip the data
    destination_directory = "/home/project/airflow/dags/finalassignment/staging/unzipped/"


    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)
   
    # Unzip the data
    with tarfile.open(tgz_file_path, 'r:gz') as tar_ref:
        tar_ref.extractall(destination_directory)
    print("Data has been successfully unzipped.")








# Function to extract data from CSV file
def extract_data_from_csv():
    # Path to the vehicle-data.csv file
    csv_file_path = "/home/project/airflow/dags/finalassignment/staging/unzipped/vehicle-data.csv"
   
    # Define column names since the CSV file has no header
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Vehicle code']
   
    # Read the CSV file without header
    df = pd.read_csv(csv_file_path, header=None, names=column_names)

   
    # Extract required fields
    extracted_data = df[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]
    
   
    extracted_directory = "/home/project/airflow/dags/finalassignment/staging/extracted/"

    if not os.path.exists(extracted_directory):
        os.makedirs(extracted_directory)

    # Save extracted data to a new CSV file
    extracted_data.to_csv(os.path.join(extracted_directory, "csv_data.csv"), index=False)



def extract_data_from_tsv():
    # Path to the tollplaza-data.tsv file
    tsv_file_path = "/home/project/airflow/dags/finalassignment/staging/unzipped/tollplaza-data.tsv"
   
    # Define column names since the TSV file has no header
    column_names = ['Number of axles', 'Tollplaza id', 'Tollplaza code']
   
    # Read the TSV file without header
    df = pd.read_csv(tsv_file_path, sep='\t', header=None, names=column_names)
   
    # Extract required fields
    extracted_data = df[['Number of axles', 'Tollplaza id', 'Tollplaza code']]
   
    extracted_directory = "/home/project/airflow/dags/finalassignment/staging/extracted/"

    if not os.path.exists(extracted_directory):
        os.makedirs(extracted_directory)

    # Save extracted data to a new CSV file
    extracted_data.to_csv(os.path.join(extracted_directory, "tsv_data.csv"), index=False)



def extract_data_from_fixed_width():
    # Path to the payment-data.txt file
    fixed_width_file_path = "/home/project/airflow/dags/finalassignment/staging/unzipped/payment-data.txt"
   
    # Define column widths for the fixed width file
    column_widths = [6, 25, 12, 5, 10, 4, 5]  # Adjusted widths based on the provided layout
   
    # Define column names for the extracted data
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code']
   
    # Read the fixed width file
    df = pd.read_fwf(fixed_width_file_path, widths=column_widths, names=column_names, header=None)
   
    # Extract required fields
    extracted_data = df[['Type of Payment code', 'Vehicle Code']]
   
    # Save extracted data to a new CSV file
    extracted_data.to_csv("/home/project/airflow/dags/finalassignment/staging/extracted/fixed_width_data.csv", index=False)



# Define the unzip_data task
unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag,
)




# Define the extract_data_from_csv task
extract_data_from_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)




# Define the extract_data_from_tsv task
extract_data_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)




# Define the extract_data_from_fixed_width task
extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)





# Define the consolidate_data task
consolidate_data_task = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," /home/project/airflow/dags/finalassignment/staging/extracted/csv_data.csv /home/project/airflow/dags/finalassignment/staging/extracted/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/extracted/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted/extracted_data.csv',
    dag=dag,
)

def transform_and_load_data():
    # Read the extracted_data.csv file
    extracted_data_file = "/home/project/airflow/dags/finalassignment/staging/extracted/extracted_data.csv"
    df = pd.read_csv(extracted_data_file)
    
    # Transform the vehicle_type field into capital letters
    df['Vehicle type'] = df['Vehicle type'].str.upper()
    
    # Save the transformed data to a new CSV file
    transformed_data_file = "/home/project/airflow/dags/finalassignment/staging/transformed/transformed_data.csv"
    df.to_csv(transformed_data_file, index=False)

# Define the transform_data task
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_load_data,
    dag=dag,
)


download_file(url,filename)

unzip_data_task >> extract_data_from_csv_task >> extract_data_from_tsv_task >> extract_data_from_fixed_width_task >> consolidate_data_task >> transform_data_task







