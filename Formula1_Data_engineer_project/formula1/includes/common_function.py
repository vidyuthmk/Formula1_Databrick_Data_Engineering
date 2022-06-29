# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingetion_date(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def incremental_load(input_df,table_name,p_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{table_name}")):
        input_df.write.mode("overwrite").insertInto(f"{table_name}")
    else:
        input_df.write.mode("overwrite").partitionBy(f"{p_column}").format("parquet").saveAsTable(f"{table_name}")

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

def getRaceList(input_df_list):
    race_year_list=[]
    for year in input_df_list:
        race_year_list.append(year.race_year)
    return race_year_list

# COMMAND ----------


