"""Test PySpark - fix hostname binding"""
import os
import sys

PYTHON_PATH = "C:/Users/Admin/AppData/Local/Programs/Python/Python312/python.exe"
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH

print(f"Python: {sys.version}")

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("WorkerTest")
    .config("spark.ui.enabled", "false")
    .config("spark.pyspark.python", PYTHON_PATH)
    .config("spark.pyspark.driver.python", PYTHON_PATH)
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

# Test RDD with Python worker
print("\n=== TEST: RDD map (Python workers) ===")
try:
    rdd = spark.sparkContext.parallelize([1, 2, 3], 1)
    result = rdd.map(lambda x: x * x).collect()
    print(f"Result: {result}")
    print("TEST: PASS!")
except Exception as e:
    print(f"TEST: FAIL - {e}")

spark.stop()
