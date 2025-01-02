# plik read_bgg_csv.py
import os
from pyspark.sql import SparkSession


warehouse_location = '/opt/spark/work-dir/lab_07/metastore_db'
base_filepath = '/home/spark/airflow/data/bgg/csv/'

spark = SparkSession\
        .builder\
        .master("local[2]")\
        .appName("Apache SQL and Hive")\
        .enableHiveSupport()\
        .config("spark.sql.warehouse.dir", warehouse_location)\
        .getOrCreate()

for file in os.listdir(base_filepath):
    if file.endswith(".csv"):
        df = spark.read.csv(base_filepath + file, header=True, inferSchema=True)
        print(df.show(5))
        