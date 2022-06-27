# Databricks notebook source
# MAGIC %md
# MAGIC ### read the file from the datalake 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","","data_source")
data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1. prepare the schema and read from the datalake

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
from pyspark.sql.functions import concat,col,lit

# COMMAND ----------

name_schema=StructType([StructField("forename", StringType(),True),
                        StructField("surname", StringType(),True)
                       ])

# COMMAND ----------

driver_schema=StructType([StructField("code",StringType(),True),
                          StructField("dob", DateType(),True),
                          StructField("driverId", IntegerType(),True),
                          StructField("driverRef", StringType(),True),
                          StructField("name",name_schema),
                          StructField("nationality", StringType(),True),
                          StructField("number", IntegerType(),True),
                          StructField("url", StringType(),True)
                         ])

# COMMAND ----------

drivers_df=spark.read\
.schema(driver_schema)\
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2. concatinate the name field

# COMMAND ----------

drivers_df_namecombine=drivers_df.withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3.Rename the fields and remove unwanted fields from the dataset

# COMMAND ----------

drivers_df_final=drivers_df_namecombine.withColumnRenamed("driverId","driver_id")\
                 .withColumnRenamed("driverRef","driver_ref")\
                 .withColumn("data_source",lit(f"{data_source}"))\
                 .drop("url")
drivers_df_final=add_ingetion_date(drivers_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4. write the file into the datalake.

# COMMAND ----------

drivers_df_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
