# Databricks notebook source
# MAGIC %md
# MAGIC ## drivers_standing 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,desc,rank,sum,count,when
from pyspark.sql.window import Window

# COMMAND ----------

driver_standing=spark.read.parquet(f"{presentation_folder}/race_results")

# COMMAND ----------

driver_rank=Window.partitionBy('race_year').orderBy(desc("points"))

# COMMAND ----------

final_df=driver_standing.groupBy("race_year","driver_name","team")\
                                                .agg(sum("points").alias("Points"),count(when(col("position")==1,True)).alias("Wins"))

# COMMAND ----------

final_df=final_df.withColumn("rank",rank().over(driver_rank))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------


