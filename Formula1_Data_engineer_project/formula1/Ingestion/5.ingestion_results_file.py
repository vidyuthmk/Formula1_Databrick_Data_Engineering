# Databricks notebook source
# MAGIC %md
# MAGIC ### Tranform the file to parquet file 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","","data_source")
data_source=dbutils.widgets.get("data_source")

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
.json(f"{raw_folder_path}/results.json") 

# COMMAND ----------

display(results_df)

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

display(results_df_1)

# COMMAND ----------

results_df_2=results_df_1.withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("data_source",lit(f"{data_source}"))\
                                    .drop("statusId")
results_df_final=add_ingetion_date(results_df_2)

# COMMAND ----------

display(results_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC Step4 . insert the data into datalake in parquet with partitioned by race_id

# COMMAND ----------

results_df_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")
