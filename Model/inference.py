# Databricks notebook source
# MAGIC %md
# MAGIC ##Import

# COMMAND ----------

import mlflow
import pandas as pd
from pyspark.sql.functions import lower, col, split, when, countDistinct

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fonction de transformation

# COMMAND ----------

def infDf(path):
    return spark.read.csv(path, sep=';', header = True)

# COMMAND ----------

def transformationInf(df):
    #Skills
    Skills_tmp = df.select(col("Name").alias("Name_s") ,"Skills")
    Skills_final= Skills_tmp.withColumn("Skills_0",split(Skills_tmp["Skills"],"\|").getItem(0)).withColumn("Skills_1",split(Skills_tmp["Skills"],"\|").getItem(1)).withColumn("Skills_2",split(Skills_tmp["Skills"],"\|").getItem(2)) 
    Skills_final = Skills_final.drop("Skills").na.fill(value="unknown",subset=["Skills_0","Skills_1","Skills_2"])

#Job
    Job_tmp = df.select(col("Name").alias("Name_j"),"Job") 
    Job_final=Job_tmp.withColumn("Job_0",split(Job_tmp["Job"],"\|").getItem(0)).withColumn("Job_1",split(Job_tmp["Job"],"\|").getItem(1)).withColumn("Job_2",split(Job_tmp["Job"],"\|").getItem(2)) 
    Job_final=Job_final.drop("Job").na.fill(value="unknown",subset=["Job_0","Job_1","Job_2"])

#Loyalty
    Loyalty_tmp = df.select(col("Name").alias("Name_l"),"Loyalty" )
    Loyalty_final = Loyalty_tmp.withColumn("Loyalty_0",split(Loyalty_tmp["Loyalty"],"\|").getItem(0)).withColumn("Loyalty_1",split(Loyalty_tmp["Loyalty"],"\|").getItem(1)).withColumn("Loyalty_2",split(Loyalty_tmp["Loyalty"],"\|").getItem(2)).withColumn("Loyalty_3",split(Loyalty_tmp["Loyalty"],"\|").getItem(3)) 
    Loyalty_final = Loyalty_final.drop("Loyalty").na.fill(value="unknown",subset=["Loyalty_0","Loyalty_1","Loyalty_2","Loyalty_3"])

#"Wand","Blood status"
    Rest_tmp = df.select(col("Name").alias("Name_r"), "House", col("Blood status").alias("Blood_status"), "Wand")
    Rest_tmp = Rest_tmp.na.fill(value= "unknown", subset = ["House","Blood_status","Wand"])


#Finale Features table
    tmp_1 = Skills_final.join(Job_final, Skills_final.Name_s == Job_final.Name_j)
    tmp_2 = tmp_1.join(Loyalty_final, tmp_1.Name_s == Loyalty_final.Name_l)
    tmp_3 = tmp_2.join(Rest_tmp , tmp_2.Name_s == Rest_tmp.Name_r)
    featureTable = tmp_3.drop("Name_s","Name_j","Name_l")
    finalFeature = tmp_3.drop("Name_s","Name_j","Name_l","Name_r")
    
    return finalFeature

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inference

# COMMAND ----------

# Prepare validation dataset
path = "/mnt/groupe3/Characters.csv"
val_df = infDf(path)
val_df = transformationInf(val_df)

# COMMAND ----------

val_df = val_df.toPandas()
y_val = val_df['House']
x_val = val_df.drop('House', axis=1)

# COMMAND ----------

# Load model as a PyFuncModel.
logged_model = 'runs:/13807b871068495eaf9f6a406138736a/model'
loaded_model = mlflow.pyfunc.load_model(logged_model)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prediction

# COMMAND ----------

# Predict on a Pandas DataFrame.
predictions = loaded_model.predict(x_val)
val_df['house_predicted'] = predictions
display(val_df[['House','house_predicted']])
