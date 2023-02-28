import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder.appName("API/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

url = "http://172.19.128.1:8080/players"
file = {'filename': 'data'}

response = requests.post(url, json = file)
data = response.json()

df = spark.read.json(spark.sparkContext.parallelize(data))

@pandas_udf(df.schema, functionType=PandasUDFType.GROUPED_MAP)
def replace_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace("", pd.NA)
    return df

df = df.groupby().apply(replace_empty_strings)

@pandas_udf(df.schema, functionType=PandasUDFType.GROUPED_MAP) 
def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    # drop the nulls
    df.dropna(how='any', inplace=True)
    # drop duplicates / example, its used only if needed, normally we don't have duplicates or we count them before
    df = df.drop_duplicates(subset='player', keep='first')
    return df

# drop nulls / this could be "replace instead of drop"
droped_df = df.groupby().apply(clean_nulls)

@pandas_udf("integer", functionType=PandasUDFType.SCALAR) 
def count_nulls(df: pd.Series) -> pd.Series:
    null_counts = df.isnull().sum()
    return null_counts

# count nulls
# null_counts = df.select([ count_nulls(df[c]).alias(c+"_null_count") for c in df.columns ])

# show the df
droped_df.show()
# null_counts.show()

droped_df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
.partitionBy("Country")\
.save()

# null_counts.write\
# .format("parquet").mode("append")\
# .option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/API_to_parquet/")\
# .save()