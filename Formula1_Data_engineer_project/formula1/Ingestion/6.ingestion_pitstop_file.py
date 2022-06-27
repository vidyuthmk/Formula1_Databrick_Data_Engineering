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
.json(f"{raw_folder_path}/pit_stops.json")


# COMMAND ----------

# MAGIC %md
# MAGIC Step 2. rename the columns according to needs and add ingetion_date feild

# COMMAND ----------

pitstops_df_renamed=pitstops_df.withColumnRenamed("raceId","race_id")\
                               .withColumnRenamed("driverId","driver_id")\
                                .withColumn("data_source",lit(f"{data_source}"))
pitstops_df_renamed=add_ingetion_date(pitstops_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 write the data back to datalake in parquet format

# COMMAND ----------

pitstops_df_renamed.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
