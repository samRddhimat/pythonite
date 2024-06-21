# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### convert array data into column

# COMMAND ----------

# data = [
#     (1, [[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
#     (2, [[10, 11], [12, 13, 14]]),
#     (3, [])
# ]
import pyspark.sql.functions as f
data = [
    (1, [[1, 2, 3], [4, 5, 6]]),
    (2, [[10, 11,20], [12, 13, 14]]),
    (3, [[7, 8, 9], [5,555,999]])
]

df1 = spark.createDataFrame(data = data, schema = ["id","matrix"])

df1.show()


# COMMAND ----------

df_outer_explode = df1.withColumn("inner_array",f.explode(df1.matrix))
df_outer_explode.show()

# COMMAND ----------

df_final = df_outer_explode.withColumn("inner_array",f.explode(df_outer_explode.inner_array))
df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### club similar id values to single row

# COMMAND ----------

df_new=df_final.drop("matrix")
df_new.show()

# COMMAND ----------

df_new1= df_new.withColumnRenamed('inner_array','cgpas')#.show()

# COMMAND ----------

df_clubbed = df_new1.groupBy("id").agg(f.collect_set("cgpas").alias("cgpa"))

df_clubbed.show()

# COMMAND ----------

df_clubbed.cache()

df_clubbed.createOrReplaceTempView("cgpa_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, (cgpa) from cgpa_list
