# Databricks notebook source
# import requests

# fileurl = "https://databkt-one.s3.ap-southeast-2.amazonaws.com/user.csv"
# local_path = "/dbfs/user.csv"

# # Download to DBFS
# with open(local_path, "wb") as f:
#     f.write(requests.get(fileurl).content)

# COMMAND ----------

# # df = spark.read \
# #     .format("csv") \
# #     .option("header", "true") \
# #     .option("delimiter", ",") \
# #     .load("https://databkt-one.s3.ap-southeast-2.amazonaws.com/user.csv")

# df = spark.read.format("csv").option("header", "true").load("/FilesStore/user.csv")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

usr_schema= StructType([
			StructField("uid", IntegerType(), True),
            StructField("uname", StringType(), True)
			])
usr_data = [(1,"Katie"),(1,"Katie"),(2,"Katie"),(3,"Ben"),(2,"Ben"),(2,"Ben")]

usr_df=spark.createDataFrame(usr_data,usr_schema)

usr_df.show()

# COMMAND ----------

usr_df.createOrReplaceTempView("usrtbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from usrtbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select uid, row_number() over(partition by uid order by uname ) as udi from usrtbl) where udi = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), count(1), count(2),count(3), count('seeni') from usrtbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT uid, uname AS PRODUCT_NAME FROM usrtbl
# MAGIC ORDER BY PRODUCT_NAME 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT uid, uname AS PRODUCT_NAME FROM usrtbl
# MAGIC ORDER BY uname 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT uname AS PRODUCT_NAME, SUM(uid) AS TOTAL_COST FROM usrtbl
# MAGIC GROUP BY PRODUCT_NAME 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT uname AS PRODUCT_NAME, SUM(uid) AS TOTAL_COST FROM usrtbl
# MAGIC GROUP BY uname 
# MAGIC

# COMMAND ----------

id_schema= StructType([
			StructField("uuid", IntegerType(), False)
			])
id_data = [(1,),(1,),(1,),(1,),(1,),(1,)]

id_df=spark.createDataFrame(id_data,id_schema)

id_df.show()

# COMMAND ----------

id_df.createOrReplaceTempView("new_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from new_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM new_table a
# MAGIC INNER JOIN new_table b
# MAGIC ON a.uuid = b.uuid;