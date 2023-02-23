import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("API to HDFS").master("spark://localhost:7077").getOrCreate()

url = "https://api.covid19api.com/summary"
response = requests.get(url)

data = response.json()["results"]

df = spark.read.json(spark.sparkContext.parallelize(data))

df.printSchema()
df.show()

deduped_df = df.dropDuplicates() # can be also used with a subset of columns ["name", "age"] for example or the distinct() function

deduped_df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
.partitionBy("Country")\
.save()