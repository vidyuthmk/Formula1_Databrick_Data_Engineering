# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circut.csv file

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
# MAGIC Step 1. read the CSV file using spark data source reader 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from pyspark.sql.functions import col,lit

# COMMAND ----------

circuit_schema=StructType([StructField("circuitId", IntegerType(), False),
                          StructField("circuteRef", StringType(), True),
                          StructField("name", StringType(), True),
                          StructField("location", StringType(), True),
                          StructField("country", StringType(), True),
                          StructField("lat", DoubleType(), True),
                          StructField("lng", DoubleType(), True),
                          StructField("alt", IntegerType(), True),
                           StructField("url", StringType(), True)
                          ])

# COMMAND ----------

circute_df=spark.read\
.option("header", True)\
.schema(circuit_schema)\
.csv(f"{raw_folder_path}/{file_date}/circuits.csv")

# COMMAND ----------

# MAGIC  %md
# MAGIC step 2. Select only required feilds

# COMMAND ----------

circute_df_reqired= circute_df.select(col("circuitid"),col("circuteRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC step 3. rename the columns as required 

# COMMAND ----------

circute_df_renamed=circute_df_reqired.withColumnRenamed("circuitid","circuit_id")\
.withColumnRenamed("circuteRef","circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumn("data_source",lit(f"{data_source}"))\
.withColumnRenamed("alt", "altitude")\
.withColumn("file_date",lit(f"{file_date}"))

# COMMAND ----------

# MAGIC %md 
# MAGIC step 4. adding new coloum ingestion data and time 

# COMMAND ----------

circuit_final_df=add_ingetion_date(circute_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC step 5. write the circuit csv file to processed data

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
