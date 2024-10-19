import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
#Por alguna razon solo estan funcionando los comandos cuando inicio esto, entonces poner en todos los archivos
