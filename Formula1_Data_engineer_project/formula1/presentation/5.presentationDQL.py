# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_df=spark.read.parquet(f"{processed_folder_path}/races")
circute_df=spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers")
team_df=spark.read.parquet(f"{processed_folder_path}/constructors")
result_df=spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

race_df=race_df.withColumnRenamed("name","race_name")\
       .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

circute_df=circute_df.withColumnRenamed("name","circuit_name")\
            .withColumnRenamed("location","circuit_location")

# COMMAND ----------

drivers_df=drivers_df.withColumnRenamed("name","driver_name")\
                    .withColumnRenamed("number","driver_number")\
                    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

result_df=result_df.withColumnRenamed("time","race_time")

# COMMAND ----------

team_df=team_df.withColumnRenamed("name","team")

# COMMAND ----------

race_circuit_df=race_df.join(circute_df,"circuit_id","inner")

# COMMAND ----------

race_result_df=result_df.join(race_circuit_df,race_circuit_df.race_id==result_df.race_id)\
                        .join(team_df,team_df.constructor_id==result_df.constructor_id)\
                        .join(drivers_df,drivers_df.driver_id==result_df.driver_id)\
                        .select(race_circuit_df["race_id"],race_circuit_df["race_name"],race_circuit_df["race_year"],race_circuit_df["circuit_location"]\
                                                ,team_df["team"],drivers_df["driver_number"],drivers_df["driver_name"],drivers_df["driver_nationality"]\
                                                  ,result_df["grid"],result_df["fastest_lap"],result_df["race_time"],result_df["points"],result_df["position"])


# COMMAND ----------

final_presentaion_df=race_result_df.withColumn("created_date",current_timestamp())



# COMMAND ----------

final_presentaion_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder}/race_results"))

# COMMAND ----------


