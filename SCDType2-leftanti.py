# Databricks notebook source
# MAGIC %md
# MAGIC ### Implementing SCD Type 2 in PySpark
# MAGIC
# MAGIC 1️⃣ Create Source (New Incoming Data) and Target (Existing Dimension) Tables

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit

# 🔹 New incoming records (Source Table)
source_data = [
    (1, "Alice", "New York"),  # Unchanged
    (2, "Bob", "Los Angeles"),  # Updated city
    (4, "Eve", "Chicago")  # New record
]
source_df = spark.createDataFrame(source_data, ["id", "name", "city"])
source_df.show()


# COMMAND ----------

# 🔹 Existing Dimension Table (Target Table)
target_data = [ (1, "Alice", "New York", "2023-01-01", None),  (2, "Bob", "Chicago", "2023-01-01", None), (3, "Carol", "Houston", "2023-01-01", None)  ] # All are Active records 
target_schema = StructType([ StructField("id", StringType(), True), StructField("name", StringType(), True),
                             StructField("city", StringType(), True),StructField("start_date", StringType(), True),
                             StructField("end_date", StringType(), True)])
target_df = spark.createDataFrame(target_data, target_schema)
target_df.show()

# COMMAND ----------

# 2️⃣ Find New or Updated Records Using LEFT ANTI JOIN. We use LEFT ANTI JOIN to get records in source_df that do not exist in target_df (new or changed data).
# 🔹 Find new or changed records
new_records_df = source_df.alias("s").join(
    target_df.alias("t"), (col("s.id") == col("t.id")) & (col("s.city") == col("t.city")), "left_anti"  # Only keeps records that don’t match
)
new_records_df.show()

# COMMAND ----------

# 3️⃣ Handle Existing Records (Update End Date for Changed Records)
from pyspark.sql.functions import when
# 🔹 Mark old records as expired
updated_target_df = target_df.withColumn(
    "end_date", when(target_df.id.isin([row["id"] for row in new_records_df.collect()]), lit("2024-03-30")).otherwise(None)
)
updated_target_df.show()

# COMMAND ----------

#4️⃣ Insert New Records with a New Start Date
from datetime import date
# 🔹 Add new start_date column
new_records_with_date_df = new_records_df.withColumn("start_date", lit(str(date.today()))).withColumn("end_date", lit(None))
new_records_with_date_df.show()

# COMMAND ----------

# 🔹 Append new records to target table
final_target_df = updated_target_df.union(new_records_with_date_df)
final_target_df.orderBy("id").show()
