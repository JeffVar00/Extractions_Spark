from pyspark.sql import SparkSession
import pyodbc
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

#from sqlalchemy import create_engine, text

spark = SparkSession.builder.appName("SQL/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

jdbcURL = "jdbc:sqlserver://172.19.128.1:1433;databaseName=AdventureWorks2019"
table = 'Person.Address'
user = 'jeff'
pwd = '1234'
df = spark.read.format("jdbc")\
    .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
    .option('url', jdbcURL)\
    .option('dbtable', table)\
    .option('user', user)\
    .option('password', pwd)\
    .load()

@pandas_udf(df.schema, functionType=PandasUDFType.GROUPED_MAP)
def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    df.dropna(how='any', inplace=True)
    return df

df = df.groupby().apply(drop_nulls)

df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/SQL_to_parquet/")\
.partitionBy("City")\
.save()


