# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21","File Date")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col,desc,rank
from pyspark.sql.window import Window

# COMMAND ----------

cons_df=spark.read.format("delta").load(f"{presentation_folder}/race_results")\
                    .filter(f"file_date='{file_date}'")

# COMMAND ----------

const_rank=Window.partitionBy("race_year").orderBy(desc("points"))

# COMMAND ----------

const_df=cons_df.groupby("race_year","team")\
                       .agg(sum("points").alias("Points"),count(when(col("position")==1,True)).alias("Wins"))


# COMMAND ----------

final_df=const_df.withColumn("rank", rank().over(const_rank))

# COMMAND ----------

race_year=final_df.select("race_year").distinct().collect()

# COMMAND ----------

race_years=getRaceList(race_year)

# COMMAND ----------

final_df=final_df.filter(col("race_year").isin(race_years))

# COMMAND ----------

merge_condition="tgt.team=src.team and tgt.race_year=src.race_year"

# COMMAND ----------

mergedata(presentation_folder,"constructor_standing",final_df,'f1_presentation.constructor_standing','race_year',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from f1_presentation.constructor_standing
# MAGIC where race_year=2014

# COMMAND ----------


