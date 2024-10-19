import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

spark = SparkSession.builder.appName("OperacionesBasicas").getOrCreate()

#In this basic case we create the data with the same structure that we will define in columns
data = [
    ("Alice", 34, "F"),
    ("Bob", 45, "M"),
    ("Catherine", 29, "F"),
    ("David", 40, "M"),
    ("Eve", 27, "F")
]

df = spark.createDataFrame(data,["Name", "Age", "Gender"])

#Stage 1
#Add column with coditions
df = df.withColumn(
    "Age_Category",
    when(col("Age") < 30, "Young").otherwise("Adult")
)

#Stage 2
#filter by age more than 30
df_filtered = df.filter(col("Age") > 30)

#Stage 3
#select some columns
df_selected = df_filtered.select("Name","Age","Age_Category")

#Stage 4
#Group by gender and average age
df_average = df.groupBy("Gender").agg(avg("Age").alias("Age_average"))

# Show results
print("DataFrame original:")
df.show()

print("DataFrame filtered and modified:")
df_selected.show()

print("Age average by gender:")
df_average.show()

# End SparkSession
spark.stop()

