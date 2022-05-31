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

# MAGIC %md ### Cleaning data
# MAGIC ####Formater la data pour la rendre utilisable 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Features creation

# COMMAND ----------

#Skills
Skills_tmp=characters.select(col("Name").alias("Name_s") ,"Skills")
Skills_final=Skills_tmp.withColumn("Skills_0",split(Skills_tmp["Skills"],"\|").getItem(0)).withColumn("Skills_1",split(Skills_tmp["Skills"],"\|").getItem(1)).withColumn("Skills_2",split(Skills_tmp["Skills"],"\|").getItem(2)) 
Skills_final=Skills_final.drop("Skills").na.fill(value="unknown",subset=["Skills_0","Skills_1","Skills_2"])

#Job
Job_tmp=characters.select(col("Name").alias("Name_j"),"Job") 
Job_final=Job_tmp.withColumn("Job_0",split(Job_tmp["Job"],"\|").getItem(0)).withColumn("Job_1",split(Job_tmp["Job"],"\|").getItem(1)).withColumn("Job_2",split(Job_tmp["Job"],"\|").getItem(2)) 
Job_final=Job_final.drop("Job").na.fill(value="unknown",subset=["Job_0","Job_1","Job_2"])

#Loyalty
Loyalty_tmp=characters.select(col("Name").alias("Name_l"),"Loyalty" )
Loyalty_final=Loyalty_tmp.withColumn("Loyalty_0",split(Loyalty_tmp["Loyalty"],"\|").getItem(0)).withColumn("Loyalty_1",split(Loyalty_tmp["Loyalty"],"\|").getItem(1)).withColumn("Loyalty_2",split(Loyalty_tmp["Loyalty"],"\|").getItem(2)).withColumn("Loyalty_3",split(Loyalty_tmp["Loyalty"],"\|").getItem(3)) 
Loyalty_final=Loyalty_final.drop("Loyalty").na.fill(value="unknown",subset=["Loyalty_0","Loyalty_1","Loyalty_2","Loyalty_3"])

#"Wand","Blood status"
Rest_tmp = characters.select(col("Name").alias("Name_r"), "House", col("Blood status").alias("Blood_status"), "Wand")
Rest_tmp = Rest_tmp.na.fill(value= "unknown", subset = ["House","Blood_status","Wand"])


#Finale Features table
tmp_1 = Skills_final.join(Job_final, Skills_final.Name_s == Job_final.Name_j)
tmp_2 = tmp_1.join(Loyalty_final, tmp_1.Name_s == Loyalty_final.Name_l)
tmp_3 = tmp_2.join(Rest_tmp , tmp_2.Name_s == Rest_tmp.Name_r)
featureTable = tmp_3.drop("Name_s","Name_j","Name_l")
finalFeature = tmp_3.drop("Name_s","Name_j","Name_l","Name_r")
#Show
finalFeature.show()

# COMMAND ----------

#finalFeature.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/tables/groupe3/finalFeature01.csv") 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Features Table Creation

# COMMAND ----------

from databricks.feature_store import feature_table
from databricks.feature_store import FeatureStoreClient

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

fs.create_table(
  name="groupe3.featureTable01md",
  primary_keys= ["Name_r"],
  df = featureTable,
  description='featureTable01md'
)

# COMMAND ----------

fs.write_table(
  name='groupe3.featureTable01md',
  df = featureTable,
  mode = 'overwrite'
)

# COMMAND ----------

ft = fs.get_table("groupe3.featureTable01md")
print(ft.keys)
print(ft.description)

# COMMAND ----------

df=fs.read_table("groupe3.featureTable01md")
display(df)
