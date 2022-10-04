# Databricks notebook source
# MAGIC %run /Users/guillermo.roman-arrudi@external-basf.com/common/enviroment

# COMMAND ----------

# MAGIC %run /Users/guillermo.roman-arrudi@external-basf.com/common/functions

# COMMAND ----------

dbutils.fs.ls("/mnt/challenge")[:10]


# COMMAND ----------

# DBTITLE 1,Files2Process
tgz_list = [a.path.lstrip("dbfs:") for a in dbutils.fs.ls("/mnt/challenge")]

# COMMAND ----------

tgz_list[:2]

# COMMAND ----------

# MAGIC %time
# MAGIC for tgz in tgz_list[:2]:
# MAGIC     df_binary = spark.read.format("binaryFile")\
# MAGIC                 .load(tgz)\
# MAGIC                 .withColumn("path", regexp_replace(col("path"),"dbfs:","/dbfs") )
# MAGIC     schema_is = ["file_path", "filename","xml_value"]
# MAGIC     
# MAGIC     df_data = df_binary.rdd.flatMap(lambda row: extract_tar(row[0])).toDF(schema_is) 
# MAGIC     df_data = df_data.repartition(10)
# MAGIC     df_data.write.mode("append").format("delta").saveAsTable("l_bronze.raw_data")

# COMMAND ----------


