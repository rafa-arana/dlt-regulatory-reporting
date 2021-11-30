# Databricks notebook source
# MAGIC %pip install git+https://github.com/rafa-arana/fire.git

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.DLT-SAN-Setup">STAGE 0</a>: Setup
# MAGIC + <a href="$./00.DLT-SAN-File Ingestion with Autoloader">STAGE 0 bis</a>: File Ingestion with Autoloader
# MAGIC + <a href="$./01.DLT-SAN-Autoloader-template">STAGE 1</a>: Data Reliable Pipelines with Live Tables and Autoloader
# MAGIC + <a href="$./03.Querying the Delta Live Tables event log">STAGE 3</a>: Querying the Delta Live Tables event log
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Load CSV files from Cloud in Streaming Mode

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Diving into the code👇

# COMMAND ----------

# MAGIC %md
# MAGIC We retrieve the name of the entity to get the FIRE data model for as well as the directory (distributed file storage) where we expect new raw files to land. These parameters are passed to the delta live table notebook via job configuration as per the screenshot above.

# COMMAND ----------

try:
  # the name of the fire entity we want to process
  fire_entity = spark.conf.get("fire_entity")
except:
  raise Exception("Please provide [fire_entity] as job configuration")
 
try:
  # where new data file will be received
  landing_zone = spark.conf.get("landing_zone")
except:
  raise Exception("Please provide [landing_zone] as job configuration")
  
try:
  # where corrupted data file will be stored
  invalid_format_path = spark.conf.get("invalid_format_path")
except:
  raise Exception("Please provide [invalid_format_path] as job configuration")
 
try:
  # format we ingest raw data
  file_format = spark.conf.get("file_format", "csv")
except:
  raise Exception("Please provide [file_format] as job configuration")
  
try:
  # format we ingest raw data
  delimiter = spark.conf.get("delimiter", "|")
except:
  raise Exception("Please provide [delimiter] as job configuration")
 
try:
  # number of new file to read at each iteration
  max_files = int(spark.conf.get("max_files", "1"))
except:
  raise Exception("Please provide [max_files] as job configuration")
  

# COMMAND ----------

# DLT import
import dlt
from pyspark.sql.functions import *

# Pyspark functions
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType, TimestampType, DecimalType, LongType

from fire.spark import FireModel

# COMMAND ----------

ingestionDate = current_date()
ingestionTime = current_timestamp()  

# COMMAND ----------

schemas_dir = "/dbfs/Users/rafael.arana@databricks.com/DLT/dlt_san/entities/"
fire_model = FireModel(schemas_dir).load(fire_entity)
fire_schema = fire_model.schema
fire_constraints = fire_model.constraints

# COMMAND ----------

# DBTITLE 1,Ingest files with Autoloader into Bronze layer
@dlt.table(
  name=fire_entity+"_bronze",
  comment="This is an incremental streaming source from autoloader csv files on ADLS",
  table_properties={
                    "delta.autoOptimize.optimizeWrite" : "true",
                    "quality" : "bronze"
  })
def get_cloudfiles():
  return (
    spark.readStream 
      .format("cloudFiles") 
      .option("cloudFiles.format",file_format)
      .option("badRecordsPath", invalid_format_path)
      .option("rescuedDataColumn", "_rescue")
      .option("cloudFiles.maxFilesPerTrigger", max_files)
      .option("header", "true")
      .option("delimiter", delimiter)
      .schema(fire_schema)
      .load(landing_zone)
      .withColumn("InputFileName",input_file_name())
      .withColumn("IngestionDate",ingestionDate)
      .withColumn("IngestionTime",ingestionTime)   
    )

# COMMAND ----------

# DBTITLE 1,Silver table with clean and validated data
@dlt.table(
    name=fire_entity+"_silver",
    comment="This is an incremental streaming source from Auto loader csv files on ADLS",
    table_properties={
                    "delta.autoOptimize.optimizeWrite" : "true",
                    "quality" : "silver"
  })
@dlt.expect_all_or_drop(dict(zip(fire_constraints, fire_constraints)))
def silver():
  return dlt.read_stream(fire_entity+"_bronze")

# COMMAND ----------

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation I’m 
  return [name for name, success in zip(fire_constraints, expectations) if not success]

# COMMAND ----------

# DBTITLE 1,Quarantine table with invalid data
@dlt.table(
    name=fire_entity+"_quarantine",
    comment="This is an incremental streaming source from Auto loader csv files on ADLS" 
)
def quarantine():
  return (
      dlt
        .read_stream(fire_entity+"_bronze")
        .withColumn("_fire", array([expr(value) for value in fire_constraints]))
        .withColumn("_fire", failed_expectations("_fire"))
        .filter(size("_fire") > 0)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.DLT-SAN-Setup">STAGE 0</a>: Setup
# MAGIC + <a href="$./00.DLT-SAN-File Ingestion with Autoloader">STAGE 0 bis</a>: File Ingestion with Autoloader
# MAGIC + <a href="$./01.DLT-SAN-Autoloader-template">STAGE 1</a>: Data Reliable Pipelines with Live Tables and Autoloader
# MAGIC + <a href="$./03.Querying the Delta Live Tables event log">STAGE 3</a>: Querying the Delta Live Tables event log
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.