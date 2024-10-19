import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read CSV").getOrCreate()

df_csv = spark.read.csv("assets/sample_data.csv", header=True, inferSchema=True)

df_csv.show()

spark.stop()