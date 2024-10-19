import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("select SQL").getOrCreate()

data = [
    ("Alice", 34, "F"),
    ("Bob", 45, "M"),
    ("Catherine", 29, "F"),
    ("David", 40, "M"),
    ("Eve", 27, "F")
]

df = spark.createDataFrame(data, ["Name", "Age", "Gender"])

#Create a temporal table
df.createOrReplaceTempView("people")

#Execute SQL
result = spark.sql("Select gender, AVG(Age) AS Average_age FROM people GROUP BY Gender")

result.show()

spark.stop()