# Databricks notebook source
# MAGIC %md
# MAGIC ### Tranform the file to delta table file 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","","data_source")
data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-28","File Date")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1. create your own schema for the file and read the data from the data lake

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DataType,DoubleType,FloatType
from pyspark.sql.functions import col,lit

# COMMAND ----------

result_schema=StructType([StructField("constructorId", IntegerType(),False),
                          StructField("driverId", IntegerType(),False),
                          StructField("fastestLap", IntegerType(),False),
                          StructField("fastestLapSpeed", StringType(),False),
                          StructField("fastestLapTime", StringType(),False),
                          StructField("grid", IntegerType(),False),
                          StructField("laps", IntegerType(),False),
                          StructField("milliseconds", IntegerType(),False),
                          StructField("number", IntegerType(),False),
                          StructField("points", FloatType(),False),
                          StructField("position", IntegerType(),False),
                          StructField("positionOrder", IntegerType(),False),
                          StructField("positionText", StringType(),False),
                          StructField("raceId", IntegerType(),False),
                          StructField("rank", IntegerType(),False),
                          StructField("resultId", IntegerType(),False),
                          StructField("time", StringType(),False),
                          StructField("statusId", IntegerType(),False),
                          
])

# COMMAND ----------

results_df=spark.read\
.schema(result_schema)\
.json(f"{raw_folder_path}/{file_date}/results.json") 

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 rename the fields as required and drop the status_id feild 

# COMMAND ----------

results_df_1=results_df.withColumnRenamed("resultId","result_id")\
                            .withColumnRenamed("raceId","race_id")\
                            .withColumnRenamed("driverId","driver_id")\
                            .withColumnRenamed("constructorId","constructor_id")\
                            .withColumnRenamed("positionText","position_text")\
                            .withColumnRenamed("positionOrder","position_order")\
                            .withColumnRenamed("fastestLap","fastest_lap")

# COMMAND ----------

results_df_2=results_df_1.withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("data_source",lit(f"{data_source}"))\
                                    .withColumn("file_date",lit(f"{file_date}"))\
                                    .drop("statusId")
results_df_final=add_ingetion_date(results_df_2)

# COMMAND ----------

results_df_final=results_df_final.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.results

# COMMAND ----------

# MAGIC %md
# MAGIC Step4 . insert the data into datalake in parquet with partitioned by race_id

# COMMAND ----------

merge_condition="tgt.result_id=src.result_id AND tgt.race_id=src.race_id"

# COMMAND ----------

mergedata(processed_folder_path,'results',results_df_final,"f1_processed.results",'race_id',merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
