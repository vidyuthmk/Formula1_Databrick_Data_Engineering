# Databricks notebook source
# MAGIC %md
# MAGIC ### Read Json File constructor.json

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","","data_source")
data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

from pyspark.sql.types import StringType,StructType,StructField,IntegerType,DoubleType,DateType
from pyspark.sql.functions import col,lit

# COMMAND ----------

constructor_schema=StructType([
    StructField("constructorId", IntegerType(), False),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
    
])

# COMMAND ----------

constructor_df = spark.read\
.schema(constructor_schema)\
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC step 2 rename and use only required fields

# COMMAND ----------

constructor_df_modified=constructor_df[col("constructorId").alias("constructor_id"),col("constructorRef").alias("constructor_ref"),col("name"),col("nationality")]

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3. add ingestion date time field

# COMMAND ----------

constructor_df_final=add_ingetion_date(constructor_df_modified)
constructor_df_final=constructor_df_final.withColumn("data_source",lit(f"{data_source}"))

# COMMAND ----------

# MAGIC %md 
# MAGIC step 4. write the modified dataset to adls datalake.

# COMMAND ----------

constructor_df_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
