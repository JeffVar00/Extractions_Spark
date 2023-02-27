import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("API to HDFS").master("spark://localhost:7077").getOrCreate()

url = "localhost:8080/players"
file = {'filename': 'data'}

response = requests.post(url, json = file)
data = response.json()["results"]

df = spark.read.json(spark.sparkContext.parallelize(data))

df.printSchema()
df.show()

# Use isNull to create boolean columns for each column
null_cols = [df[col].isNull().alias(col+"_null") for col in df.columns]

# Use sum to count the number of null values in each column
null_counts = df.select(null_cols).agg(*[sum(col).alias(col+"_null_count") for col in df.columns])

# Select only the null count columns
null_counts.select([col for col in null_counts.columns if "_null_count" in col]).show()

deduped_df = df.dropDuplicates() # can be also used with a subset of columns ["name", "age"] for example or the distinct() function

deduped_df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
.partitionBy("Country")\
.save()

null_counts.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
.save()