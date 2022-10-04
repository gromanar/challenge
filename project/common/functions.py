# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import *
import tarfile
from io import BytesIO
from typing import Iterator

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.spark.xml.functions.from_xml
# MAGIC import com.databricks.spark.xml.schema_of_xml
# MAGIC import spark.implicits._

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

@pandas_udf("string")
def extract_title_udf(array_: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for a in array_:
        try:
            if isinstance(a.get("_lang"),list) and a.get("_lang").endswith("en"):
                yield a.get("_VALUE")
        except:
            yield a
