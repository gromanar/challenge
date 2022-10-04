# Databricks notebook source
# MAGIC %run /Users/guillermo.roman-arrudi@external-basf.com/common/enviroment

# COMMAND ----------

# MAGIC %run /Users/guillermo.roman-arrudi@external-basf.com/common/functions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from l_bronze.raw_data

# COMMAND ----------

# MAGIC %scala
# MAGIC val xmlSchema = schema_of_xml(
# MAGIC   spark.sql("select xml_value from l_bronze.raw_data").as[String])
# MAGIC // use scala xml parser
# MAGIC spark.sql("select * from l_bronze.raw_data").withColumn("parsed_xml", from_xml($"xml_value", xmlSchema)).createOrReplaceGlobalTempView("binarydfParsed")

# COMMAND ----------

# DBTITLE 1,read global data
df_parsed = spark.sql("select * from global_temp.binarydfParsed")

# COMMAND ----------

df_parsed.rdd.getNumPartitions()

# COMMAND ----------

display(df_parsed.select("parsed_xml").withColumn("invention-title",col("parsed_xml").getItem("bibliographic-data").getItem("invention-title")))

# COMMAND ----------

array_example = [{"_VALUE": "VERFAHREN ZUR HERSTELLUNG VON SULFONSÄUREESTER", "_format": "original", "_id": "title_de", "_lang": "de"}, {"_VALUE": "PROCESS FOR PRODUCTION OF SULFONIC ACID ESTER", "_format": "original", "_id": "title_en", "_lang": "en"}, {"_VALUE": "PROCÉDÉ DE FABRICATION D'UN ESTER D'ACIDE SULFONIQUE", "_format": "original", "_id": "title_fr", "_lang": "fr"}]

# COMMAND ----------

[x.get('_VALUE') for x in array_example if x.get("_lang").endswith("en")]

# COMMAND ----------

( 
    df_parsed
     .withColumn("invention-title",col("parsed_xml").getItem("bibliographic-data").getItem("invention-title"))
     .select("invention-title")
 .rdd.flatMap(lambda array: [x.get('_VALUE') for x in array if x.get("_lang").endswith("en")],preservesPartitioning=True)
 )
 


# COMMAND ----------

@udf(returnType=StringType())
def seek_eng_title(array):
    for a in array:
        if a.getItem("_lang")==lit("en"):
            return a.getItem("_VALUE")
        else:
            return "bad title"
        

# COMMAND ----------

display(df_parsed
 .withColumn("invention-title",col("parsed_xml").getItem("bibliographic-data").getItem("invention-title"))
        #  .withColumn("title",seek_eng_title(col("invention-title")) )
        
        .select("invention-title")
        .withColumn("title",map_filter(col("invention-title"), lambda x,y: x.getItem("_lang") 
                   )
                    )
       )

# COMMAND ----------

df_parsed_extr = (
    df_parsed
        .withColumn("abstract", col("parsed_xml").getItem("abstract"))
        .withColumn("invention-title",col("parsed_xml").getItem("bibliographic-data").getItem("invention-title"))
        .withColumn("title",
                    when(exists(col("invention-title"),lambda x: x.getItem("_lang").contains("en")),col("invention-title").getItem(1).getItem("_VALUE"))
                    .otherwise(col("invention-title").getItem(0).getItem("_VALUE"))
                   )
        .withColumn("is_title_en",
                    when(exists(col("invention-title")
                                ,lambda x: x.getItem("_lang").contains("en")),col("invention-title").getItem(1).getItem("_lang"))
                    .otherwise(col("invention-title").getItem(0).getItem("_VALUE"))
                   )
        .withColumn("publication-of-grant-date",col("parsed_xml").getItem("bibliographic-data").getItem("dates-of-public-availability").getItem("publication-of-grant-date").getItem("date"))
        .withColumn("is_qualified",
                    when(
                            ( (array_contains((col("abstract").getItem("_lang")),"en")) & (col("is_title_en").contains("en"))
                        ),True)
                    .otherwise(False))
                 )

# COMMAND ----------

display(df_parsed_extr.select("abstract","invention-title","title","is_title_en","is_qualified"))

# COMMAND ----------

display(df_parsed_extr.select("abstract","invention-title","title","is_title_en","is_qualified").filter(col("is_qualified") ==False))

# COMMAND ----------

# array_test = [{"_VALUE": "AUF SICHTBARES LICHT ANSPRECHENDER PHOTOKATALYSATOR, HERSTELLUNGSVERFAHREN DAFÜR, PHOTOKATALYSATORBESCHICHTUNGSMITTEL UNTER VERWENDUNG DAVON UND PHOTOKATALYSATORDISPERSION", "_format": "original", "_id": "title_de", "_lang": "de"}, {"_VALUE": "VISIBLE LIGHT-RESPONSIVE PHOTOCATALYST, METHOD FOR PRODUCING SAME, PHOTOCATALYST COATING AGENT USING SAME, AND PHOTOCATALYST DISPERSION", "_format": "original", "_id": "title_en", "_lang": "en"}, {"_VALUE": "PHOTOCATALYSEUR SENSIBLE A LA LUMIERE VISIBLE, SON PROCEDE DE PRODUCTION, AGENT DE REVETEMENT DE PHOTOCATALYSEUR L'UTILISANT ET DISPERSION DE PHOTOCATALYSEUR", "_format": "original", "_id": "title_fr", "_lang": "fr"}]

# COMMAND ----------

# def extract_test(array_):
#     yield [a.get("_VALUE") for a in array_ if isinstance(a.get("_lang"),str) and a.get("_lang").endswith("en")]
# for a in extract_test(array_test):
#     print(a)

# COMMAND ----------

display(df_parsed_extr
        .select("invention-title")
#  extract title 
        .withColumn("invention-title-en",
                    when(exists(col("invention-title"),lambda x: x.getItem("_lang").contains("en")),col("invention-title").getItem(1).getItem("_VALUE"))
                    .otherwise(col("invention-title").getItem(0).getItem("_VALUE"))
                   )
       )

# COMMAND ----------

display(df_parsed_extr.select("invention-title")
        .withColumn("title",
                    when(f.array_contains((f.col("abstract").getItem("_lang")),"en")
                         col("invention-title").getItem(0))
        (f.array_contains((f.col("abstract").getItem("_lang")),"en"))
        
       )

# COMMAND ----------

# DBTITLE 1,persist
# MAGIC %time
# MAGIC for tgz in tgz_list[:2]:
# MAGIC     df_binary = spark.read.format("binaryFile")\
# MAGIC                 .load(tgz)\
# MAGIC                 .withColumn("path", regexp_replace(col("path"),"dbfs:","/dbfs") )
# MAGIC     schema_is = ["file_path", "filename","xml_value"]
# MAGIC     
# MAGIC     df_data = df_binary.rdd.flatMap(lambda row: extract_tar(row[0])).toDF(schema_is) 
# MAGIC     df_data = df_data.repartition(10)
# MAGIC     df_data.write.mode("append").format("delta").saveAsTable("l_silver.raw_data")
