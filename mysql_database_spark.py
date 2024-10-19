import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read MySQL Data") \
    .config("spark.jars", "assets/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()
    
# Database connection properties
jdbc_url = "jdbc:mysql://localhost:3306/spark_db"
db_table = "employees"
connection_properties = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL table
df_mysql = spark.read.jdbc(url=jdbc_url, table=db_table, properties=connection_properties)

# Show the data
df_mysql.show()

# Stop SparkSession
spark.stop()