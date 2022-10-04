# Databricks notebook source
df = spark.sql('select * from l_silver.qualified_data')

# COMMAND ----------

cluster = "http://10.2.0.4"
port = "9200"
elasticsearchIndex= "guillermo"

df
 .select("abstract","title","publication_year")\ 
 .write.format( "org.elasticsearch.spark.sql" )\ 
 .option( "es.nodes",cluster)\ 
 .option( "es.port",port)\ 
 .option( "es.nodes.wan.only", "true" )\ 
 .mode("overwrite")\ 
 .save(f"index/{elasticsearchIndex}")
 
