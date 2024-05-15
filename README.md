# Airflow_project
Apache airflow ETL

This project uses apache airflow to unzip and treat a zipped folder through a pipeline of tasks in apache airflow.

The python script dowloads the tar file to use.

Before running this script you must start apache airflow with the command 
```bash
start_airflow
```
and opening the Ui of airflow. Open a terminal and create a directory structure for staging area as follows:
/home/project/airflow/dags/finalassignment/staging.
```bash
sudo mkdir -p /{your_desired_path}/airflow/dags/finalassignment/staging
```

Note: If you face any permission issues in writing to the directories, please execute the following commands:
```bash
sudo chown -R 100999 /home/project/airflow/dags/finalassignment
sudo chmod -R g+rw /home/project/airflow/dags/finalassignment  
sudo chown -R 100999 /home/project/airflow/dags/finalassignment/staging
sudo chmod -R g+rw /home/project/airflow/dags/finalassignment/staging
```
Change to your work directory:
```bash
cd /home/project/airflow/dags/finalassignment/staging
```
