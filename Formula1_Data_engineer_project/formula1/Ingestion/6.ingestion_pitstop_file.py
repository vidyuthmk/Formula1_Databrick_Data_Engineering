# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingesting Pit stops data multiline csv files

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","","data_source")
data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21","File Date")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1. define the schema for the data 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import lit

# COMMAND ----------

pitstop_schema=StructType([StructField("raceId", IntegerType(),False),
                          StructField("driverId", IntegerType(),True),
                          StructField("stop", IntegerType(),True),
                          StructField("lap", IntegerType(),True),
                           StructField("time",StringType(),True),
                          StructField("duration", StringType(),True),
                          StructField("milliseconds", IntegerType(),True),
                           
                          ])

# COMMAND ----------

pitstops_df=spark.read\
.schema(pitstop_schema)\
.option("multiLine",True)\
.json(f"{raw_folder_path}/{file_date}/pit_stops.json")


# COMMAND ----------

# MAGIC %md
# MAGIC Step 2. rename the columns according to needs and add ingetion_date feild

# COMMAND ----------

pitstops_df_renamed=pitstops_df.withColumnRenamed("raceId","race_id")\
                               .withColumnRenamed("driverId","driver_id")\
                               .withColumn("data_source",lit(f"{data_source}"))\
                               .withColumn("file_date",lit(f"{file_date}"))
pitstops_df_renamed=add_ingetion_date(pitstops_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 write the data back to datalake in parquet format

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.pit_stops 

# COMMAND ----------

pit_stop_final=reorder_partioned_column(pitstops_df_renamed,'race_id')

# COMMAND ----------

incremental_load(pit_stop_final,"f1_processed.pit_stops",'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*)
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")
