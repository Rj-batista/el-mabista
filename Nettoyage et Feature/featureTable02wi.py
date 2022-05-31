# Databricks notebook source
import os
from pyspark.sql.functions import lower, col, split, when, countDistinct
import sys
print([dbutils.fs.ls("/mnt/groupe3/")[i][1] for i in range(len(dbutils.fs.ls("/mnt/groupe3/")))])
def load_data(name):
    return spark.read.csv("/mnt/groupe3/{}.csv".format(name),sep=';',header=True)

# COMMAND ----------

characters=load_data("Characters")
characters = characters.na.drop(subset=["House"])
characters.show()

# COMMAND ----------

eye = characters.select(col("Name").alias("Name_e"), col("Eye colour").alias("eye_colour"))
hair = characters.select(col("Name").alias("Name_h"), col("Hair colour").alias("hair_colour"))
Hair_colour_Eye_colour_Final = eye.join(hair, eye.Name_e == hair.Name_h)
Hair_colour_Eye_colour_Final = Hair_colour_Eye_colour_Final.drop("Name_h").na.fill(value = "unknown", subset = ["eye_colour", "hair_colour"])

# COMMAND ----------

from databricks.feature_store import feature_table
from databricks.feature_store import FeatureStoreClient

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

fs.create_table(
  name="groupe3.featureTable02wi",
  primary_keys= ["Name_e"],
  df = Hair_colour_Eye_colour_Final,
  description='featureTable02wi'
)

# COMMAND ----------

fs.write_table(
  name='groupe3.featureTable02wi',
  df = Hair_colour_Eye_colour_Final,
  mode = 'overwrite'
)
