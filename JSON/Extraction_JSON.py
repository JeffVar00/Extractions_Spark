from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import time

# SparkSession
spark = SparkSession.builder.appName("JSON to HDFS").getOrCreate()

start = time.time()

# read JSON file into a DataFrame
df = spark.read.option("inferSchema", "true").option("header", "true").option("multiline","true").json("Employees.json")

# show the DataFrame
df.printSchema()
df.show()

# replace null values in specific columns with a default value, this creates a new df
df = df.select(
    when(col("name").isNull(), "Unknown").otherwise(col("name")).alias("name"),
    when(col("age").isNull(), 30).otherwise(col("age")).alias("age"),
    when(col("department").isNull(), "Unspecified").otherwise(col("department")).alias("department"),
    *[ when(col(colu).isNull(), "N/A").otherwise(col(colu)).alias(colu) for colu in df.columns if colu not in ["name", "age", "department"] ]
)

end = time.time()
print(" HEYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY " + str(end - start) )

# write the DataFrame to HDFS
df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/JSON_to_parquet/")\
.partitionBy("department")\
.save()

