# Databricks notebook source
dbutils.secrets.list('formula1-scope')

# COMMAND ----------

storage_account_name  = "formula1newdatalake"
client_id             = dbutils.secrets.get('formula1-scope','databricksap-client-id')
tenant_id             = dbutils.secrets.get('formula1-scope','databricks-tenant-id')
client_secret         = dbutils.secrets.get('formula1-scope','databricks-client-secret')

# COMMAND ----------

config = {"fs.azure.account.auth.type":"OAuth",
          "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id":f"{client_id}",
          "fs.azure.account.oauth2.client.secret":f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint":f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mountadls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs =config)

# COMMAND ----------

mountadls("raw")

# COMMAND ----------

mountadls("processed")

# COMMAND ----------

mountadls("presentation")

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1newdatalake/raw")

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1newdatalake/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1newdatalake/raw")
