# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.DLT-SAN-Setup">STAGE 0</a>: Setup
# MAGIC + <a href="$./00.DLT-SAN-File Ingestion with Autoloader">STAGE 0 bis</a>: File Ingestion with Autoloader
# MAGIC + <a href="$./01.DLT-SAN-Autoloader-template">STAGE 1</a>: Data Reliable Pipelines with Live Tables and Autoloader
# MAGIC + <a href="$./03.Querying the Delta Live Tables event log">STAGE 3</a>: Querying the Delta Live Tables event log
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rafa-arana/fire.git

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from fire.spark import FireModel
# Pyspark functions
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType, TimestampType, DecimalType, LongType


# COMMAND ----------

# MAGIC %md # JM_CLIENT_BII

# COMMAND ----------

# DBTITLE 1,Sample input configuration for the pipeline
fire_entity = "client"
landing_zone = "/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/client"
invalid_format_path = "/Users/rafael.arana@databricks.com/DLT/dlt_san/invalid/client"
file_format = "csv"
max_files = "1"

# COMMAND ----------

fire_model = FireModel("/dbfs/Users/rafael.arana@databricks.com/DLT/dlt_san/entities/").load(fire_entity)
client_schema = fire_model.schema
client_constraints = fire_model.constraints

# COMMAND ----------

client_constraints

# COMMAND ----------

# DBTITLE 1,Ingest client CSV files with Autoloader using the JSON schema
ingestionDate = current_date()
ingestionTime = current_timestamp()
  
df1 = spark.readStream \
          .format("cloudFiles") \
          .option("cloudFiles.format",file_format) \
          .option("badRecordsPath", invalid_format_path) \
          .option("rescuedDataColumn", "_rescue") \
          .option("cloudFiles.maxFilesPerTrigger", max_files) \
          .option("header", "true") \
          .option("delimiter", "|") \
          .schema(client_schema) \
          .load(landing_zone) \
          .withColumn("InputFileName",input_file_name()) \
          .withColumn("IngestionDate",ingestionDate) \
          .withColumn("IngestionTime",ingestionTime)
display(df1)

# COMMAND ----------

# MAGIC %md # JM_FLUJOS

# COMMAND ----------

# DBTITLE 1,Sample input configuration for the pipeline
fire_entity = "flujos"
landing_zone = "/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/flujos"
invalid_format_path = "/Users/rafael.arana@databricks.com/DLT/dlt_san/invalid/flujos"
file_format = "csv"
delimiter = "|"
max_files = "1"

# COMMAND ----------

fire_model = FireModel("/dbfs/Users/rafael.arana@databricks.com/DLT/dlt_san/entities/").load(fire_entity)
flujos_schema = fire_model.schema
flujos_constraints = fire_model.constraints

# COMMAND ----------

flujos_constraints

# COMMAND ----------

# DBTITLE 1,Ingest flujos CSV files with Autoloader using the JSON schema
ingestionDate = current_date()
ingestionTime = current_timestamp()

flujosDF = spark.readStream \
          .format("cloudFiles") \
          .option("cloudFiles.format",file_format) \
          .option("badRecordsPath", invalid_format_path) \
          .option("rescuedDataColumn", "_rescue") \
          .option("cloudFiles.maxFilesPerTrigger", max_files) \
          .option("cloudFiles.backfillInterval", "30 seconds") \
          .option("header", "true") \
          .option("delimiter", delimiter) \
          .schema(flujos_schema) \
          .load(landing_zone) \
          .withColumn("InputFileName",input_file_name()) \
          .withColumn("IngestionDate",ingestionDate) \
          .withColumn("IngestionTime",ingestionTime)

display(flujosDF)

# COMMAND ----------

# MAGIC %md # JM_INTERV_CTO

# COMMAND ----------

# DBTITLE 1,Sample input configuration for the pipeline
fire_entity = "interviniente"
landing_zone = "/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/interviniente"
invalid_format_path = "/Users/rafael.arana@databricks.com/DLT/dlt_san/invalid/interviniente"
file_format = "csv"
delimiter = "|"
max_files = "1"

# COMMAND ----------

fire_model = FireModel("/dbfs/Users/rafael.arana@databricks.com/DLT/dlt_san/entities/").load(fire_entity)
interv_schema = fire_model.schema
interv_constraints = fire_model.constraints

# COMMAND ----------

# DBTITLE 1,Ingest intervinientes CSV files with Autoloader using the JSON schema
ingestionDate = current_date()
ingestionTime = current_timestamp()

intervDF = spark.readStream \
          .format("cloudFiles") \
          .option("cloudFiles.format",file_format) \
          .option("rescuedDataColumn", "_rescue") \
          .option("header", "true") \
          .option("delimiter", delimiter) \
          .schema(interv_schema) \
          .load(landing_zone) \
          .withColumn("InputFileName",input_file_name()) \
          .withColumn("IngestionDate",ingestionDate) \
          .withColumn("IngestionTime",ingestionTime)

display(intervDF)

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