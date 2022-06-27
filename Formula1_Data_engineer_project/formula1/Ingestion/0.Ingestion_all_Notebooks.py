# Databricks notebook source
dbutils.notebook.run("./1.ingestion_circuts_file",0,{"data_source":"Eargast_api"})

# COMMAND ----------

dbutils.notebook.run("./2.ingestion_race_file",0,{"data_source":"Eargast_api"})

# COMMAND ----------

dbutils.notebook.run("./3.ingestion_consturctor_file",0,{"data_source":"Eargast_api"})

# COMMAND ----------

dbutils.notebook.run("./4.ingestion_drivers_file",0,{"data_source":"Eargast_api"})

# COMMAND ----------

dbutils.notebook.run("./5.ingestion_results_file",0,{"data_source":"Eargast_api"})

# COMMAND ----------

dbutils.notebook.run("./6.ingestion_pitstop_file",0,{"data_source":"Eargast_api"})

# COMMAND ----------

dbutils.notebook.run("./7.ingestion_lap_times_file",0,{"data_source":"Eargast_api"})

# COMMAND ----------

dbutils.notebook.run("./8.ingestion_Qualitfying_file",0,{"data_source":"Eargast_api"})
