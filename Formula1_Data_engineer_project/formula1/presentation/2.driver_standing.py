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

driver_standing=spark.read.format("delta").load(f"{presentation_folder}/race_results")\
                            .filter(f"file_date='{file_date}'")

# COMMAND ----------

driver_rank=Window.partitionBy('race_year').orderBy(desc("points"))

# COMMAND ----------

final_df=driver_standing.groupBy("race_year","driver_name","driver_id","team","file_date")\
                                                .agg(sum("points").alias("Points"),count(when(col("position")==1,True)).alias("Wins"))

# COMMAND ----------

final_df=final_df.withColumn("rank",rank().over(driver_rank))

# COMMAND ----------

final_df_list=final_df.select("race_year").distinct()\
                 .collect()

# COMMAND ----------

race_year_list=getRaceList(final_df_list)

# COMMAND ----------

final_df=final_df.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

final_df=final_df.dropDuplicates(['driver_id'])

# COMMAND ----------

merge_condition="tgt.race_year =src.race_year and tgt.driver_id=src.driver_id"

# COMMAND ----------

mergedata(presentation_folder,"driver_standings",final_df,'f1_presentation.driver_standings','race_year',merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
