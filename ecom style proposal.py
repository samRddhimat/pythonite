# Databricks notebook source
# MAGIC %md
# MAGIC ### ecom style proposal
# MAGIC For all orders containing multiple products, find the top 5 most common product pairs that are frequently bought together.
# MAGIC Do it in an optimized way that avoids cartesian explosion!
# MAGIC
# MAGIC **collect_set, tuple, struct are used**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, count, asc, array, collect_set, explode, struct,concat
from itertools import combinations

from pyspark.sql.types import ArrayType,StructType,StructField, StringType
from pyspark.sql.functions import udf

trans = [
    ("O001", "P001"), ("O001", "P002"), ("O001", "P003"),
    ("O002", "P001"), ("O002", "P002"),
    ("O003", "P004"),
    ("O004", "P001"), ("O004", "P003")
]

dftrans = spark.createDataFrame(trans, ["order_id", "product_id"])

dftrans.show()

# COMMAND ----------

dforders = dftrans.groupBy("order_id") \
    .agg(collect_set("product_id").alias("products"))

dforders.show()

# COMMAND ----------

def generate_pairs(products):
    return [tuple(sorted(p)) for p in combinations(products,2)] if len(products)>=2 else[]
    # return [tuple(p) for p in combinations(products,2)] if len(products)>=2 else[]

# COMMAND ----------

pair_udf = udf(generate_pairs, ArrayType(StructType([StructField("_1", StringType()),
                                                     StructField("_2", StringType())])))

# COMMAND ----------

# dftest =dforders.withColumn("pairs", pair_udf("products")).select(explode("pairs").alias("pair"))
# dftest.show()

# COMMAND ----------

dfpair=dforders.withColumn("pairs", pair_udf("products")).select(explode("pairs").alias("pair"))
dfpair=dfpair.select(col("pair._1").alias("product_1"), col("pair._2").alias("product_2"))`
dfpair.show()

# COMMAND ----------

dfresult=dfpair\
        .withColumn("product_pair",struct(col("product_1"), col("product_2")))\
            .groupBy("product_1", "product_2").agg(count("*").alias("count")).orderBy(col("count")\
                .desc())
# dfresult.show()

# COMMAND ----------

dfresult = dfresult.withColumn("product_pair",struct(col("product_1"), col("product_2"))).drop(col("product_1"), col("product_2"))
dfresult.show()