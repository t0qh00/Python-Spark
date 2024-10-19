import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col #this is used in Stage 4

spark = SparkSession.builder.appName("OperacionesBasicas").getOrCreate()

#In this basic case we create the data with the same structure that we will define in columns
data = [
    ("Alice", 34, "F", 3000),
    ("Bob", 45, "M", 4000),
    ("Cathy", 29, "F", 3500),
    ("David", 35, "M", 4500),
    ("Eva", 28, "F", 3800)
]
columns = ["Name", "Age", "Gender", "Salary"]
#df = data frame
df = spark.createDataFrame(data,columns)

#Stage 1
#show the data in the data frame
df.show()

#Stage 2
#how to select specific columns in spark
df.select("Name","Salary").show()

#Stage 3
#how to filter by columns
df.filter(df["Age"] > 30).show()

#Stage 4
#how to add new columns if isn't in the original data
df = df.withColumn("Tax", col("Salary") * 0.10).show()

#Stage 5
#how to group by columns and average they salary
df.groupBy("Gender").avg("Salary").show()

#Stage 6
#how to order by column
df.orderBy(df["Salary"].desc()).show()
