# Databricks notebook source
# Function to add current_timestamp field to the table or dataframe

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingetion_date(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

# Funtion to add the data to the table in incremental load

# COMMAND ----------

def incremental_load(input_df,table_name,p_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{table_name}")):
        input_df.write.mode("overwrite").insertInto(f"{table_name}")
    else:
        input_df.write.mode("overwrite").partitionBy(f"{p_column}").format("parquet").saveAsTable(f"{table_name}")

# COMMAND ----------


# Funtion to reorder the schema inorder to support partition data 

# COMMAND ----------

def reorder_partioned_column(input_df,coloumn):
    name_list=input_df.schema.names
    for i in range(len(name_list)):
        if name_list[i]==coloumn:
            temp=name_list[-1]
            name_list[-1]=coloumn
            name_list[i]=temp
    input_df.schema.names=name_list
    input_df=input_df.select(name_list)
    return(input_df)

# COMMAND ----------

# function to get the race list in oder overite only those rows with matching years

# COMMAND ----------

def getRaceList(input_df_list):
    race_year_list=[]
    for year in input_df_list:
        race_year_list.append(year.race_year)
    return race_year_list

# COMMAND ----------

# Function to merge the data to the existing table (new delta table ability)

# COMMAND ----------

from delta.tables import *
def mergedata(file_path,file_name,source_df,table_name,p_column,merge_condition):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    if (spark._jsparkSession.catalog().tableExists(f"{table_name}")):
        deltaTable= DeltaTable.forPath(spark, f"{file_path}/{file_name}")
        deltaTable.alias("tgt").merge(
        source_df.alias("src"),f'{merge_condition}')\
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        source_df.write.mode("overwrite").partitionBy(f"{p_column}").format("delta").saveAsTable(f"{table_name}")
    
    
    

# COMMAND ----------


