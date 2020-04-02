from pyspark.sql import SparkSession                                                               
from class_SparkHelper import SparkHelper
import pyspark

# Get a spark session / context
spark_context, sql_context, hive_context = SparkHelper.get_contexts(name='exasol_test', queue='adhoc')

def exasol_connector(spark):
  # SQL query
  qStr = "SELECT REPOSITORY, POSITION, MEMBER_ID, CONVERSION_RATE \
      FROM table"
  
  # Add your own database host, username and password. NOTE: You must use the local IP of the first data node. (10.xxx)
  exaDF = spark.read.format("exasol")\
      .option("host", "********")\
      .option("port", "8563")\
      .option("username", "SeanDev11")\
      .option("password", "*******")\
      .option("query", qStr).load()
      
  return exaDF

if __name__ == "__main__":
  
  print("entered main")
  
  spark = SparkSession.builder.appName("exasol_test").getOrCreate()
  extractedDF = exasol_connector(spark)
      
  # Perform analysis on extractedDF here.
  
  spark.stop()
  

