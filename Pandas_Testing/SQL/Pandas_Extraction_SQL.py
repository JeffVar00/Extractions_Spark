from pyspark.sql import SparkSession
import pyodbc
from pyspark.sql.functions import pandas_udf, PandasUDFType

#from sqlalchemy import create_engine, text

spark = SparkSession.builder.appName("SQL/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

def transformation_df(df):
    # show the dataframe, the first 10 values
    print(df.head(10))

    # show if there is any missing values
    print(df.isnull().sum())

    # pandas version
    df.dropna(how='any', inplace=True)

    # axis ->
    #   0 or ‘index’ : Drop rows which contain missing values.
    #   1 or ‘columns’ : Drop columns which contain missing value.
    #
    # how ->
    #   ‘any’ : If any NA values are present, drop that row or column.
    #   ‘all’ : If all values are NA, drop that row or column.

    print(df.isnull().sum())

    return df


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
def transform_df_to_udf(pdf):
    return transformation_df(pdf)

df_spark = df.apply(transform_df_to_udf)


df_spark.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/SQL_to_parquet/")\
.partitionBy("City")\
.save()


