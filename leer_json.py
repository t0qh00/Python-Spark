import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read JSON").getOrCreate()

df_json = spark.read.option("multiline", "true").json("assets/sample_data.json")

df_json.show()

spark.stop()