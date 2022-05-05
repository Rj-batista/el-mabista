# Databricks notebook source
import matplotlib.pyplot as plt
import seaborn as sns
import os
import pandas as pd

# COMMAND ----------

df = spark.read.csv("/mnt/groupe3/Characters.csv",sep=';',header=True)
#df.cache()

display(df)
#df.createOrReplaceTempView("data_hp")



# COMMAND ----------

list = dbutils.fs.ls('dbfs:/mnt/groupe3')
dfs = spark.createDataFrame(list).select('*')

dfs.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Gender from data_hp where House = "Slytherin" and Gender IS NOT NULL;

# COMMAND ----------

df.toPandas()

# COMMAND ----------

df = pd.DataFrame({'mass': [0.330, 4.87 , 5.97],
                   'radius': [2439.7, 6051.8, 6378.1]},
                  index=['Mercury', 'Venus', 'Earth'])
plot = df.plot.pie(y='mass', figsize=(5, 5))

# COMMAND ----------

path, dirs, files = next(os.walk("/groupe3/"))
file_count = len(files)

# COMMAND ----------


