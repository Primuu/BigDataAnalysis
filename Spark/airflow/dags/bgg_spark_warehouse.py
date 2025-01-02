# plik bgg_spark_warehouse.py
# przykład operatora Sensora
import pendulum
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.sensors.filesystem import FileSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from pyspark.sql import SparkSession
from pyspark import SparkContext
from airflow.operators.bash import BashOperator


@dag(
    schedule=timedelta(hours=4),
    start_date=pendulum.datetime(2024, 12, 11, tz="UTC"),
    catchup=False,
    tags=["bgg"],
)
def bgg_save_to_warehouse():
    base_filepath = '/home/spark/airflow/data/bgg/csv/'
    processed_filepath = '/home/spark/airflow/data/bgg/processed/'

    # jeżeli ścieżka wskazuje na folder to nasłuchiwane będzie pojawienie się
    # jakiegokolwiek pliku w tym folderze
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='fs_bgg',
        filepath='csv',
        poke_interval=10,
        timeout=300
    )

    # @task.pyspark(conn_id="spark_default")
    # def read_csv_with_spark(spark: SparkSession, sc: SparkContext):
    #     print("Spark task!")
    #     for file in os.listdir(base_filepath):
    #         if file.endswith(".csv"):
    #             print("Plik odnaleziony!")
    #             print(f"{base_filepath + file}")
    #             print(spark.sparkContext)
    #             df = spark.read.csv(base_filepath + file, header=True, inferSchema=True)
    #             # print(df.show(5))

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_read_csv',
        # application='/opt/spark/work-dir/spark_scripts/read_bgg_csv.py',
        application='/home/spark/airflow/dags/read_bgg_csv.py',
        conn_id=None,
        executor_cores=2,
        executor_memory='2g',
        num_executors=2,
        name='airflow-spark',
        conf={
        'spark.master': 'local',
        'spark.submit.deployMode': 'client'
    }
    )

    move_csv_file = BashOperator(
        task_id='move_csv_file',
        bash_command=f'mv {base_filepath}*.csv {processed_filepath}',
    )
    
    
    chain(wait_for_file, spark_submit_task, move_csv_file)
    

bgg_save_to_warehouse()
