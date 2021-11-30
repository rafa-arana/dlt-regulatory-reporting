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

# MAGIC %md ## Load sample data

# COMMAND ----------

# DBTITLE 1,JM_CLIENT_BII_20211031 - Sample Data
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/client/M_CLIENT_BII_20211032.csv","""
feoperac|s1emp|idnumcli|tip_pers|carter|clisegm|clisegl1|clisegl2|tipsegl2|utp_cli|finiutcl|ffinutcl
2021-10-31|23100|000545685|00011|00002|00006|00275|00010|C|99999|0001-01-01|9999-12-31
2021-10-31|23100|000700897|00001|00002|00006|00275|00010|C|99999|0001-01-01|9999-12-31
2021-10-31|23100|000836297|00001|00002|00006|00223|00009|B|99999|0001-01-01|9999-12-31
2021-10-31|23100|001069718|00001|00002|00006|00188|00008|A|99999|0001-01-01|9999-12-31
2021-10-31|23100|001148887|00001|00002|00006|00133|00007|S|99999|0001-01-01|9999-12-31
2021-10-31|23100|001183889|00001|00002|00006|00184|00008|A|99999|0001-01-01|9999-12-31
""",overwrite=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat "/dbfs/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/client/M_CLIENT_BII_20211032.csv"

# COMMAND ----------

# MAGIC %md ## Define the schema and constraints

# COMMAND ----------

# DBTITLE 1,Client business entity definition - schema and constraints as a JSON
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/dlt_san/entities/client.json","""
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "title": "Client Schema",
  "description": "Data schema to define client.",
  "type": "object",
  "properties": {
    "feoperac": {
      "description": "Formatted as YYYY-MM-DDTHH:MM:SSZ in accordance with ISO 8601.",
      "type": "string",
      "format": "date"
    },
    "s1emp": {
      "description": ".",
      "type": "string"
    },
    "idnumcli": {
      "description": ".",
      "type": "string"
    },
    "tip_pers": {
      "description": ".",
      "type": "integer",
      "minimum": 1,
      "maximum": 9
    },
    "carter": {
      "description": ".",
      "type": "string"
    },
    "clisegm": {
      "description": ".",
      "type": "string"
    },
    "clisegl1": {
      "description": ".",
      "type": "string"
    },
    "clisegl2": {
      "description": ".",
      "type": "string"
    },
    "tipsegl2": {
      "description": ".",
      "type": "string",
      "enum": [
        "CORRENTISTA",
        "FOLHA",
        "SEM SEGMENTO",
        "N",
        "S"
      ]
    },
    "utp_cli": {
      "description": ".",
      "type": "string"
    },
    "finiutcl": {
      "description": "Formatted as YYYY-MM-DDTHH:MM:SSZ in accordance with ISO 8601.",
      "type": "string",
      "format": "date"
    },
    "ffinutcl": {
      "description": "Formatted as YYYY-MM-DDTHH:MM:SSZ in accordance with ISO 8601.",
      "type": "string",
      "format": "date"
    }
  },
  "required": ["feoperac", "s1emp", "idnumcli","tip_pers","carter","clisegm","clisegl1","clisegl2","tipsegl2","utp_cli","finiutcl","ffinutcl"],
  "additionalProperties": true
}
""",overwrite=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat "/dbfs//Users/rafael.arana@databricks.com/DLT/dlt_san/entities/client.json"

# COMMAND ----------

# MAGIC %md ## Build the model with Fire API

# COMMAND ----------

# MAGIC %md
# MAGIC The Fire API will convert the JSON metadata definition into a model with an schema and constraints

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

# MAGIC %md
# MAGIC Let's review the contraints build from the metadata (JSON)

# COMMAND ----------

client_constraints

# COMMAND ----------

# MAGIC %md # JM_FLUJOS

# COMMAND ----------

# MAGIC %md ## Load sample data

# COMMAND ----------

# DBTITLE 1,JM_FLUJOS_20211031 - Sample data
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/flujos/JM_FLUJOS_20211031.csv","""feoperac|s1emp|contra1|fecmvto|clasmvto|importe|salonbal
2021-08-31|04851|000000034|2021-08-01|00009|000000000000000.00|-000000000008476.33
2021-08-31|04851|000000034|2021-08-02|00009|000000000000000.00|-000000000008476.33
2021-08-31|04851|000000034|2021-08-03|00009|-000000000000209.39|-000000000008598.32
2021-08-31|04851|000000034|2021-08-04|00009|000000000000000.00|-000000000008506.93
2021-08-31|04851|000000034|2021-08-05|00009|000000000000000.00|-000000000008506.93
2021-08-31|04851|000000034|2021-08-06|00009|000000000000000.00|-000000000008506.93
2021-08-31|04851|000000034|2021-08-07|00009|000000000000000.00|-000000000008506.93
2021-08-31|04851|000000034|2021-08-08|00009|000000000000000.00|-000000000008506.93
2021-08-31|04851|000000034|2021-08-09|00009|000000000000000.00|-000000000008506.93""",overwrite=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat "/dbfs/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/flujos/JM_FLUJOS_20211031.csv"

# COMMAND ----------

# MAGIC %md ## Define the schema and constraints

# COMMAND ----------

# DBTITLE 1,Flujos business entity definition - schema and constraints as a JSON
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/dlt_san/entities/flujos.json","""
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "title": "Client Schema",
  "description": "Data schema to define flujos.",
  "type": "object",
  "properties": {
    "feoperac": {
      "description": "Fecha Operación.Formatted as YYYY-MM-DD.",
      "type": "string",
      "format": "date"
    },
    "s1emp": {
      "description": "s1emp description.",
      "type": "string"
    },
    "contra1": {
      "description": "contra1 description.",
      "type": "string"
    },
    "fecmvto": {
      "description": "Fecha vencimiento.Formatted as YYYY-MM-DD",
      "type": "string",
      "format": "date"
    },
    "clasmvto": {
      "description": "clasmvto description.",
      "type": "string"
    },
    "importe": {
      "description": "Description importe. A positive/negative number in minor units (cents/pence)",
      "type": "number",
      "monetary": true
    },
    "salonbal": {
      "description": "salon bal description.",
      "type": "string"
    }
  },
  "required": ["feoperac", "s1emp", "contra1","fecmvto","clasmvto","importe","salonbal"],
  "additionalProperties": true
}
""",overwrite=True)

# COMMAND ----------

# MAGIC %md ## Build the model with Fire API

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

# MAGIC %md # JM_INTERV_CTO

# COMMAND ----------

# MAGIC %md ## Load sample data

# COMMAND ----------

# MAGIC %md ## Define the schema and constraints

# COMMAND ----------

# DBTITLE 1,JM_INTERV_CTO_20211031 - Sample data
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/interviniente/JM_INTERV_CTO_20211031.csv","""feoperac|s1emp|contra1|tipintev|tipintv2|numordin|idnumcli|formintv
2021-10-31|23100|001365291|00001|00006|00000000001.000000|080032636|99999
2021-10-31|23100|000063379|00001|00006|00000000001.000000|040076040|99999
2021-10-31|23100|001365452|00001|00006|00000000001.000000|020208278|99999
2021-10-31|23100|000065603|00001|00006|00000000001.000000|070521213|99999
2021-10-31|23100|001365670|00001|00006|00000000001.000000|050140039|99999
2021-10-31|23100|000108833|00001|00006|00000000001.000000|060066887|99999
2021-10-31|23100|001365679|00001|00006|00000000001.000000|007361168|99999
2021-10-31|23100|000139775|00001|00006|00000000001.000000|040551899|99999
2021-10-31|23100|001366027|00001|00006|00000000001.000000|000108111|99999
2021-10-31|23100|000145575|00001|00006|00000000001.000000|004616741|99999
2021-10-31|23100|001366982|00001|00006|00000000001.000000|050442017|99999""",overwrite=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat "/dbfs/Users/rafael.arana@databricks.com/DLT/dlt_san/landing/interviniente/JM_INTERV_CTO_20211031.csv"

# COMMAND ----------

# DBTITLE 1,Interv business entity definition - schema and constraints as a JSON
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/dlt_san/entities/interviniente.json","""
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "title": "Interviniente Schema",
  "description": "Data schema to define interviniente.",
  "type": "object",
  "properties": {  
    "feoperac": {
      "description": "Fecha Operación.Formatted as YYYY-MM-DD.",
      "type": "string",
      "format": "date"
    },
    "s1emp": {
      "description": "s1emp description.",
      "type": "string"
    },
    "contra1": {
      "description": "contra1 description.",
      "type": "string"
    },
    "tipintev": {
      "description": "tipintev description",
      "type": "string"
    },
    "tipintv2": {
      "description": "tipintv2 description.",
      "type": "string"
    },
    "numordin": {
      "description": "numordin Description",
      "type": "string"
    },
    "idnumcli": {
      "description": "idnumcli description.",
      "type": "string"
    },
    "formintv": {
      "description": "formintv description.",
      "type": "string"
    }
  },
  "required": ["feoperac", "s1emp", "contra1","tipintev","tipintv2","numordin","idnumcli","formintv"],
  "additionalProperties": true
}
""",overwrite=True)

# COMMAND ----------

# MAGIC %md ## Build the model with Fire API

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

interv_schema

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