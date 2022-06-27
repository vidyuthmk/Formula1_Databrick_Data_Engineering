# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingesting Qualifying data multiline json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

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

qulifying_schema=StructType([StructField("raceId", IntegerType(),False),
                     StructField("qualifyId", IntegerType(),False),
                     StructField("driverId", IntegerType(),True),
                     StructField("constructorId", IntegerType(),True),
                     StructField("number", IntegerType(),True),
                     StructField("position", IntegerType(),True),
                     StructField("q1", StringType(),True),
                     StructField("q2", StringType(),True),
                     StructField("q3", StringType(),True)    
                          ])

# COMMAND ----------

qulifying_df=spark.read\
.schema(qulifying_schema)\
.option("multiLine",True)\
.json(f"{raw_folder_path}/qualifying")


# COMMAND ----------

# MAGIC %md
# MAGIC Step 2. rename the columns according to needs and add ingetion_date feild

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qulifying_df_rename=qulifying_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("qualifyId","qualify_id")\
                                .withColumnRenamed("constructorId","constructor_id")\
                                .withColumn("data_source",lit(f"{data_source}"))
qualify_final_df=add_ingetion_date(qulifying_df_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 write the data back to datalake in parquet format

# COMMAND ----------

qualify_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
