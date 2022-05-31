# Databricks notebook source
import os
from pyspark.sql.functions import lower, col, split, when, countDistinct
import sys
print([dbutils.fs.ls("/mnt/groupe3/")[i][1] for i in range(len(dbutils.fs.ls("/mnt/groupe3/")))])
def load_data(name):
    return spark.read.csv("/mnt/groupe3/{}.csv".format(name),sep=';',header=True)

# COMMAND ----------

characters=load_data("Characters")
spells = load_data("Spells")
characters = characters.na.drop(subset=["House"])
characters.show()

# COMMAND ----------

# MAGIC %md ### Cleaning data
# MAGIC ####Formater la data pour la rendre utilisable 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Loyalty

# COMMAND ----------

Loyalty_tmp=characters.select(col("Name").alias("Namel"),"Loyalty","House")
Loyalty_final=Loyalty_tmp.withColumn("Loyalty_0",split(Loyalty_tmp["Loyalty"],"\|").getItem(0)).withColumn("Loyalty_1",split(Loyalty_tmp["Loyalty"],"\|").getItem(1)).withColumn("Loyalty_2",split(Loyalty_tmp["Loyalty"],"\|").getItem(2)).withColumn("Loyalty_3",split(Loyalty_tmp["Loyalty"],"\|").getItem(3)) 
Loyalty_final=Loyalty_final.drop("Loyalty").na.fill(value="unknown",subset=["Loyalty_0","Loyalty_1","Loyalty_2","Loyalty_3"])
Loyalty_final.show(90)

# COMMAND ----------

#Skills
Skills_tmp=characters.select("Name","Skills")
Skills_final=Skills_tmp.withColumn("Skills_0",split(Skills_tmp["Skills"],"\|").getItem(0)).withColumn("Skills_1",split(Skills_tmp["Skills"],"\|").getItem(1)).withColumn("Skills_2",split(Skills_tmp["Skills"],"\|").getItem(2)) 
Skills_final=Skills_final.drop("Skills").na.fill(value="unknown",subset=["Skills_0","Skills_1","Skills_2"])

#Job
Job_tmp=characters.select(col("Name").alias("Namej"),"Job") 
Job_final=Job_tmp.withColumn("Job_0",split(Job_tmp["Job"],"\|").getItem(0)).withColumn("Job_1",split(Job_tmp["Job"],"\|").getItem(1)).withColumn("Job_2",split(Job_tmp["Job"],"\|").getItem(2)) 
Job_final=Job_final.drop("Job").na.fill(value="unknown",subset=["Job_0","Job_1","Job_2"])


#Concatenation Job + Skills 

Skills_Job_Final=Skills_final.join(Job_final,Job_final.Namej ==  Skills_final.Name) 
Skills_Job_Loyalty_House_Final=Skills_Job_Final.join(Loyalty_final,Skills_Job_Final.Namej ==  Loyalty_final.Namel) 
Skills_Job_Loyalty_House_Final=Skills_Job_Loyalty_House_Final.drop("Namel","Namej").na.fill(value="unknown",subset="House")
Skills_Job_Loyalty_House_Final.show()

# COMMAND ----------

#Skills_Job_Final.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/tables/groupe3/skills_job.csv") 
#Loyalty_final.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/tables/groupe3/loyalty.csv")  
#House_final.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/tables/groupe3/house.csv")  
Skills_Job_Loyalty_House_Final.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/tables/groupe3/skills_job_loyalty_house_final.csv") 

# COMMAND ----------

tmp_spells=spells.withColumn('Type',split(spells['Type'],',').getItem(0)).withColumn('Light',split(spells['Light'],',').getItem(0))
tmp_spells.show()

# COMMAND ----------

from databricks.feature_store import feature_table
from databricks.feature_store import FeatureStoreClient

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

fs.create_table(
  name="groupe3.featureTable03rb",
  primary_keys= ["Name"],
  df = Skills_Job_Loyalty_House_Final,
  description='featureTable03rb'
)

# COMMAND ----------

fs.write_table(
  name='groupe3.featureTable03rb',
  df = Skills_Job_Loyalty_House_Final,
  mode = 'overwrite'
)
