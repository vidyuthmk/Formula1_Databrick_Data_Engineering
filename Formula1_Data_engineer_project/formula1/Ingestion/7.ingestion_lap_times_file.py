# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingesting lap times data multiline json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

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

lap_time=StructType([StructField("raceId", IntegerType(),False),
                          StructField("driverId", IntegerType(),True),
                          StructField("position", IntegerType(),True),
                          StructField("lap", IntegerType(),True),
                          StructField("time", StringType(),True),
                          StructField("milliseconds", IntegerType(),True),
                           
                          ])

# COMMAND ----------

lap_time_df=spark.read\
.schema(lap_time)\
.option("multiLine",True)\
.csv(f"{raw_folder_path}/{file_date}/lap_times")


# COMMAND ----------

# MAGIC %md
# MAGIC Step 2. rename the columns according to needs and add ingetion_date feild

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_time_df_rename=lap_time_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("data_source",lit(f"{data_source}"))\
                                .withColumn("file_date",lit(f"{file_date}"))
lap_time_df_final=add_ingetion_date(lap_time_df_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 write the data back to datalake in parquet format

# COMMAND ----------

merge_condition="tgt.driver_id=src.driver_id and tgt.race_id=src.race_id"

# COMMAND ----------

mergedata(processed_folder_path,"lap_times",lap_time_df_final,"f1_processed.lap_times",'race_id',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*)
# MAGIC from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")
