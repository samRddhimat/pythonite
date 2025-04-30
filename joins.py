# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
df1_lst = [(1, "Alice"),    (2, "Bob"),    (3, "Cathy")]
df2_lst = [(2, "Math"),    (3, "Science"),    (4, "History")]

df1_schema_lst = StructType([StructField("id", IntegerType(), True),
                             StructField("name", StringType(), True)])
df2_schema_lst = StructType([StructField("id", IntegerType(),True),
                             StructField("subject",StringType(),True)])

test_schema_lst = StructType([\
    StructField("id", IntegerType(),True) \
        ])                             
df1 = spark.createDataFrame(df1_lst, df1_schema_lst )

df2 = spark.createDataFrame(df2_lst,df2_schema_lst )

df1.printSchema()

# COMMAND ----------

# Keeps all rows from df1; null for unmatched in df2.
df1.join(df2,"id","left").show()

# COMMAND ----------

# Keeps all rows from df2; null for unmatched in df1.
df1.join(df2,"id","right").show()

# COMMAND ----------

# Keeps all rows from both; null where no match.
df1.join(df2,"id","fullouter").show()

# COMMAND ----------

# Only rows from df1 without a match in df2.
df1.join(df2,"id","leftanti").show()

# COMMAND ----------

# Only rows from df1 that have a match in df2.
df1.join(df2,"id","leftsemi").show()

# COMMAND ----------

