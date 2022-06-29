# Databricks notebook source
# MAGIC %md
# MAGIC ## drivers_standing 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21","File Date")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.functions import col,desc,rank,sum,count,when
from pyspark.sql.window import Window

# COMMAND ----------

driver_standing=spark.read.parquet(f"{presentation_folder}/race_results")\
                            .filter(f"file_date='{file_date}'")

# COMMAND ----------

driver_rank=Window.partitionBy('race_year').orderBy(desc("points"))

# COMMAND ----------

final_df=driver_standing.groupBy("race_year","driver_name","team","file_date")\
                                                .agg(sum("points").alias("Points"),count(when(col("position")==1,True)).alias("Wins"))

# COMMAND ----------

final_df_list=final_df.withColumn("rank",rank().over(driver_rank))\
                 .select("race_year").distinct()\
                 .collect()

# COMMAND ----------

race_year_list=[]
for year in final_df_list:
    race_year_list.append(year.race_year)

# COMMAND ----------

final_df=final_df.filter(col("race_year").isin(race_year_list))
display(final_df)

# COMMAND ----------

final_df=reorder_partioned_column(final_df,'race_year')

# COMMAND ----------

incremental_load(final_df,"f1_presentation.driver_standings",'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from f1_presentation.driver_standings
# MAGIC where race_year=2021

# COMMAND ----------

dbutils.notebook.exit("Success")
