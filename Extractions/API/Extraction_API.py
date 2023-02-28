import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, sum

spark = SparkSession.builder.appName("API to HDFS").master("spark://localhost:7077").getOrCreate()

url = "http://172.19.128.1:8080/players"
file = {'filename': 'data'}

response = requests.post(url, json = file)
data = response.json()

df = spark.read.json(spark.sparkContext.parallelize(data))
df = df.replace("", None)

# null count
null_cols = [sum(when(isnan(c) | col(c).isNull(), 1).otherwise(0)).alias(c+"_null_count") for c in df.columns]
null_counts = df.agg(*null_cols)

deduped_df = df.na.drop(how='any')
deduped_df = deduped_df.dropDuplicates(["player"]) # can be also used with a subset of columns ["name", "age"] for example or the distinct() function

deduped_df.show()
null_counts.show()

deduped_df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
.partitionBy("Country")\
.save()
