# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import *
import tarfile
from io import BytesIO

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.spark.xml.functions.from_xml
# MAGIC import com.databricks.spark.xml.schema_of_xml
# MAGIC import spark.implicits._

# COMMAND ----------

!curl "http://10.2.0.4:9200/_cluster/health"

# COMMAND ----------

dbutils.fs.ls("/mnt/")[:10]

# COMMAND ----------

dbutils.fs.ls("/mnt/training_data")[:10]

# COMMAND ----------

# explore txt data
txt_data = "/dbfs/mnt/training_data/energy.consumption.positive.txt"
with open (txt_data,"r") as text_:
    energy_consumption = text_.read().splitlines()
len(energy_consumption)

# COMMAND ----------

energy_consumption[:3]

# COMMAND ----------


df_energy = pd.DataFrame.from_dict(
    {"path":energy_consumption}
)
df_energy = spark.createDataFrame(df_energy)
df_energy.rdd.getNumPartitions()

# COMMAND ----------

df_energy = (df_energy
    
    .withColumn("folderName", split('path', "/")[0])
 .withColumn("fileName", split('path', "/")[1])
 .withColumn("region", substring('fileName', 0,2))
 .withColumn("date", to_date(substring('fileName', 3,6),"yyyyMM"))
)
df_energy.show(truncate = False)

# COMMAND ----------

df_energy.filter(col("date").isNull()).show()

# COMMAND ----------

df_energy.groupBy("date").count().show()

# COMMAND ----------

display(spark.read
        .format('com.databricks.spark.xml')
        .options(rowTag='questel-patent-document')
        .load('/mnt/challenge/epapat_201935_back_100001_120000.tgz'))




# COMMAND ----------

tgz_list = [a.path.lstrip("dbfs:") for a in dbutils.fs.ls("/mnt/challenge")]
tgz_list[:10]

# COMMAND ----------

tgz_list[:1]

# COMMAND ----------

import tarfile
tgz = tarfile.open("/dbfs"+tgz_list[1],"r|*")
tgz

# COMMAND ----------

for file in tgz:
    if file.isfile():
        xml = tgz.extractfile(file).read().decode()
        print(xml)

# COMMAND ----------

for tgz in tgz_list[:1]:
    df_binary = spark.read.format("binaryFile")\
                .load(tgz)\
                .withColumn("path", regexp_replace(col("path"),"dbfs:","/dbfs") )
    display(df_binary)

# COMMAND ----------

from pyspark.sql.types import MapType
@udf(returnType=MapType())
def decode_tgz(tgz):
    if t.isfile():
        xml = tgz.extractfile(file).read().decode()
        print(xml)

# COMMAND ----------

# d =  [{'name': '', 'size': '','xml':''}]
# data_rdd = sc.parallelize(d)
# myDF = spark.createDataFrame(data_rdd)
# myDF.show()

# COMMAND ----------

# /dbfs/mnt/challenge/epapat_201935_back_100001_120000.tgz

# COMMAND ----------

# sc.binaryFiles("/dbfs/mnt/challenge/epapat_201935_back_100001_120000.tgz")

# COMMAND ----------

def extract_tar(path):
    try:
        tar = tarfile.open(path, mode="r|*")
        for x in tar:         
            if x.isfile(): 
                yield path, x.name, tar.extractfile(x).read().decode()
    except tarfile.TarError:
        yield path, "Error", ""

# COMMAND ----------

schema_is = ["file_path", "filename","xml_value"]
df_data = df_binary.rdd.flatMap(lambda row: extract_tar(row[0])).toDF(schema_is) 


# COMMAND ----------

# shape
print((df_data.count(), len(df_data.columns)))


# COMMAND ----------

df_data = df_data.repartition(8)
df_data.rdd.getNumPartitions()

# COMMAND ----------

df_data.write.mode("append").format("delta").saveAsTable

# COMMAND ----------

df_data.createOrReplaceGlobalTempView("binarydf")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_data = spark.sql("select * from global_temp.binarydf")
# MAGIC val xmlSchema = schema_of_xml(df_data.select("xml_value").as[String])
# MAGIC spark.sql("select * from global_temp.binarydf").withColumn("parsed_xml", from_xml($"xml_value", xmlSchema)).createOrReplaceGlobalTempView("binarydfParsed")

# COMMAND ----------

df_data_parsed = spark.sql("select * from global_temp.binarydfParsed")

# COMMAND ----------

df_data_parsed = (
    df_data_parsed
        .withColumn("abstract", col("parsed_xml").getItem("abstract"))
        .withColumn("invention-title",col("parsed_xml").getItem("bibliographic-data").getItem("invention-title"))
        .withColumn("title",col("parsed_xml").getItem("description").getItem("element").getItem("heading").getItem("element").getItem("maths").getItem("math").getItem("title"))
        .withColumn("publication-of-grant-date",col("parsed_xml").getItem("bibliographic-data").getItem("dates-of-public-availability").getItem("publication-of-grant-date").getItem("date"))
        .withColumn("is_data_quality",
                    when(
                            ( (array_contains((col("abstract").getItem("_lang")),"en")) & (array_contains((col("invention-title").getItem("_lang")),"fr"))
                        ),True)
                    .otherwise(False))
                 )

# COMMAND ----------

display(df_data_parsed)

# COMMAND ----------

# [a.get("_VALUE") for a in array_ if a.get("_lang") and a.get("_lang").endswith("en")]
from pyspark.sql.types import StringType
@udf(returnType=StringType())
def extract_title(array_):
    return [a.get("_VALUE") for a in array_ if isinstance(a.get("_lang"),list) and a.get("_lang").endswith("en")]
    

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf
from typing import Iterator

# COMMAND ----------

@pandas_udf("string")
def extract_title_udf(array_: Iterator[pd.Series]) -> Iterator[pd.Series]:
    yield [a.get("_VALUE") for a in array_ if not isinstance(a.get("_lang"),str) and a.get("_lang").endswith("en")]


# COMMAND ----------

@pandas_udf("string")
def extract_title_udf(array_: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for a in array_:
        try:
            if isinstance(a.get("_lang"),list) and a.get("_lang").endswith("en"):
                yield a.get("_VALUE")
        except:
            yield a

display(df_data_parsed
        .select("parsed_xml")
        .filter(col("invention-title").isNotNull())
        .withColumn("invention-title-en",extract_title_udf(col("invention-title")))
#         .filter(col("invention-title-en").isNotNull())           
       )

# COMMAND ----------

display(df_data_parsed
        .select("parsed_xml")
        .filter(col("invention-title").isNotNull())
        .withColumn("invention-title-en",extract_title(col("invention-title")))
#         .filter(col("invention-title-en").isNotNull())           
       )

# COMMAND ----------



# COMMAND ----------

test_data = (df_data_parsed
        .select("parsed_xml")
        .withColumn("invention-title",col("parsed_xml").getItem("bibliographic-data").getItem("invention-title").getItem(3))
        .filter(col("invention-title").isNotNull())
         .select("invention-title"))

# COMMAND ----------

display(test_data)

# COMMAND ----------

display(df_data_parsed)

# COMMAND ----------

# DBTITLE 1,testing com.databricks:spark-xml_2.12:0.15.0
# for tgz in tgz_list[:1]:
#     display(
#         spark.read.format("com.databricks.spark.xml")
#             .load(tgz)
# #             .withColumn("path", regexp_replace(col("path"),"dbfs:","/dbfs") )
            
# )

# COMMAND ----------

df = (spark.read.format("binaryFile").load("/mnt/challenge/epapat_201935_back_340001_360000.tgz")
    .withColumn("path", f.regexp_replace(f.col("path"),"dbfs:","/dbfs")     )).drop("content")
display(df)
