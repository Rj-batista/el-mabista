# Databricks notebook source
import os
import sys
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


