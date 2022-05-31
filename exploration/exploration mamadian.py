# Databricks notebook source
!pip install pandas_profiling

# COMMAND ----------

import os
import sys
from pandas_profiling import ProfileReport
from IPython.core.display import display


# COMMAND ----------

print([dbutils.fs.ls("/mnt/groupe3/")[i][1] for i in range(len(dbutils.fs.ls("/mnt/groupe3/")))])

def load_data(name):
    return spark.read.csv("/mnt/groupe3/{}.csv".format(name),sep=';',header=True)

# COMMAND ----------

characters=load_data("Characters")
potions=load_data("Potions")
spells=load_data("Spells")
hp1= load_data("Harry Potter 1")
hp2= load_data("Harry Potter 2")
hp3= load_data("Harry Potter 3")



# COMMAND ----------

profile

# COMMAND ----------

profile = ProfileReport(characters.toPandas(), title="Pandas Profiling Report", explorative=True)

# COMMAND ----------

p = profile.to_html()

# COMMAND ----------

displayHTML(p)

# COMMAND ----------

hp1.createOrReplaceTempView("hp1")
hp2.createOrReplaceTempView("hp2")
hp3.createOrReplaceTempView("hp3")
characters.createOrReplaceTempView("characters")
potions.createOrReplaceTempView("potions")
spells.createOrReplaceTempView("spells")

# COMMAND ----------

query = """SELECT Job FROM characters GROUP BY Job
"""
characterdf_jobs = spark.sql(query)
characterdf_jobs.showall()

# COMMAND ----------


