# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col,desc,rank
from pyspark.sql.window import Window

# COMMAND ----------

cons_df=spark.read.parquet(f"{presentation_folder}/race_results")

# COMMAND ----------

const_rank=Window.partitionBy("race_year").orderBy(desc("points"))

# COMMAND ----------

const_df=cons_df.groupby("race_year","team")\
                       .agg(sum("points").alias("Points"),count(when(col("position")==1,True)).alias("Wins"))


# COMMAND ----------

final_df=const_df.withColumn("rank", rank().over(const_rank))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standing")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder}/constructor_standing"))

# COMMAND ----------


