# Databricks notebook source
import os
import requests
import numpy as np
import pandas as pd
import json 
from pyspark.sql.functions import lower, col, split, when, countDistinct

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
    Rest_tmp = df.select(col("Name").alias("Name_r"), col("Blood status").alias("Blood_status"), "Wand")
    Rest_tmp = Rest_tmp.na.fill(value= "unknown", subset = ["Blood_status","Wand"])


#Finale Features table
    tmp_1 = Skills_final.join(Job_final, Skills_final.Name_s == Job_final.Name_j)
    tmp_2 = tmp_1.join(Loyalty_final, tmp_1.Name_s == Loyalty_final.Name_l)
    tmp_3 = tmp_2.join(Rest_tmp , tmp_2.Name_s == Rest_tmp.Name_r)
    featureTable = tmp_3.drop("Name_s","Name_j","Name_l")
    finalFeature = tmp_3.drop("Name_s","Name_j","Name_l","Name_r")
    
    return finalFeature

# COMMAND ----------

def validationGenerator(df):
    jsonVal = transformationInf(df).toPandas().to_json(orient = 'records')
    return jsonVal

# COMMAND ----------

def score_model(url,token,data_json):
    
    headers = {'Authorization': f'Bearer '+token}
    
    response = requests.request(method='POST', headers=headers, url=url, json=data_json)
    
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

# COMMAND ----------

def load_data(name):
    return spark.read.csv("/mnt/groupe3/{}.csv".format(name),sep=';',header=True)

# COMMAND ----------

df = load_data("Characters_val")
validationGenerator(df)

# COMMAND ----------

df.show()

# COMMAND ----------

#mettre le contenu de json que vous voulez tester
json_test = json.loads("""[
{"Skills_0":"Chaser",
"Skills_1":" Bat-Bogey hex",
"Skills_2":"unknown",
"Job_0":"Student",
"Job_1":"unknown",
"Job_2":"unknown",
"Loyalty_0":"Dumbledore's Army ",
"Loyalty_1":" Order of the Phoenix "
,"Loyalty_2":" Hogwarts School of Witchcraft and Wizardry"
,"Loyalty_3":"unknown"
,"Blood_status":"Pure-blood"
,"Wand":"Unknown"},
{"Skills_0":"Spotting Nargles","Skills_1":"unknown","Skills_2":"unknown","Job_0":"Student","Job_1":"unknown","Job_2":"unknown","Loyalty_0":"Dumbledore\'s Army ","Loyalty_1":"Hogwarts School of Witchcraft and Wizardry","Loyalty_2":"unknown","Loyalty_3":"unknown","Blood_status":"Pure-blood\\u00a0or\\u00a0half-blood","Wand":"Unknown"}
]
""")

#Mettre l'url de l'api que vous avez mis en service 
url_api = "https://adb-8992331337369088.8.azuredatabricks.net/model/md02/1/invocations"
#Mettre le token que vous venez de créer dans ce String 
token = "dapi69ea6ff61cd019619bbd53d4d4ce008c"

score_model(url_api,token,json_test)
