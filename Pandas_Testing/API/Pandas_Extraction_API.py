import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType, col

spark = SparkSession.builder.appName("API/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

url = "localhost:8080/players"
file = {'filename': 'data'}

response = requests.post(url, json = file)
data = response.json()["results"]

df = spark.read.json(spark.sparkContext.parallelize(data))

@pandas_udf(df.schema, functionType=PandasUDFType.GROUPED_MAP)
def drop_nulls(df):
    df.dropna(how='any', inplace=True)
    return df

@pandas_udf("integer", functionType=PandasUDFType.SCALAR_ITER) 
def count_nulls(df):
    return df.isnull().sum()

droped_df = df.groupby().apply(drop_nulls)
null_counts = df.select([count_nulls(col(c)).alias(c) for c in df.columns])

null_counts.show()

droped_df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
.partitionBy("Country")\
.save()

null_counts.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
.save()