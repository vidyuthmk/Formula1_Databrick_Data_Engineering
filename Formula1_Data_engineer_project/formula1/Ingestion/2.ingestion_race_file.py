# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest race csv file

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
# MAGIC step 1. read the csv from the adls datalake 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from pyspark.sql.functions import col,to_timestamp,lit,concat

# COMMAND ----------

race_schema=StructType([StructField("raceid", IntegerType(), False),
                        StructField("year", IntegerType(), True),
                        StructField("round", IntegerType(), True),
                        StructField("circuitid", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("date", DateType(), True),
                        StructField("time", StringType(), True),
                        StructField("url", StringType(), True)
                        ])

# COMMAND ----------

race_df_raw=spark\
.read\
.option("header",True)\
.schema(race_schema)\
.csv(f"{raw_folder_path}/{file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC step 2.  Select only required feilds 

# COMMAND ----------

race_df_cols=race_df_raw[col("raceid"), col("year"),col("round"),col("circuitid"),col("name"),col("date"),col("time")]

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3. rename the coloums

# COMMAND ----------

race_df_rename=race_df_cols.withColumnRenamed("raceid","race_id")\
.withColumnRenamed("year","race_year")\
.withColumn("data_source",lit(f"{data_source}"))\
.withColumnRenamed("circuitid","circuit_id")\
.withColumn("file_date",lit(f"{file_date}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4. add new column Ingestion_date 

# COMMAND ----------

race_df_addcol=add_ingetion_date(race_df_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5. combine date and time field and comvert it into timestamp

# COMMAND ----------

race_df_timeStamp=race_df_addcol.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

race_final_df=race_df_timeStamp[col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"),col("data_source"),col("file_date")]

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6 write the file to storage 

# COMMAND ----------

race_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")
