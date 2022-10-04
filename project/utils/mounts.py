# Databricks notebook source
# MAGIC %fs ls /mnt/

# COMMAND ----------

# MAGIC %run /Users/guillermo.roman-arrudi@external-basf.com/common/enviroment

# COMMAND ----------

folder_mnt

# COMMAND ----------

# test mount
dbutils.fs.mount(
 source = "wasbs://{0}@{1}.blob.core.windows.net/".format(container, storage_account_name,folder),
 mount_point = folder_mnt,
 extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name):storage_account_key}
)
print("mount created: ",folder_mnt)

# COMMAND ----------

spark.createDataFrame(dbutils.fs.ls(folder_mnt)).show(truncate=False)

# COMMAND ----------

# dbutils.fs.unmount(folder_mnt)
