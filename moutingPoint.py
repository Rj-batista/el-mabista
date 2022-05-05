# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://groupe3@esgidatas.blob.core.windows.net",
mount_point = "/mnt/groupe3",
extra_configs = {"fs.azure.account.key.esgidatas.blob.core.windows.net":dbutils.secrets.get(scope = "ScopeESGI", key = "elmata")})


# COMMAND ----------

rawdata = spark.read.csv("/mnt/groupe3/Characters.csv",sep=';',header=True)
