from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
# import pandas as pd
from datetime import datetime

# SparkSession
spark = SparkSession.builder.appName("CSV/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

# pandas transformation from the pyspark trasnformation I did in the previous snippet
def transform_df(df):

    # replace null values in specific columns with a default value
    default_values = {'name' : "Unknown"}
    df.fillna(value=default_values, inplace=True)

    # interpolate the age column
    df["age"] = df["age"].interpolate(method="linear")

    # fill the rest of the columns with a N/A value
    df.fillna(value="N/A", inplace=True)

    df_pandas = df[['userID', 'name', 'age', 'friends']]
    df_pandas = df_pandas[df_pandas['age'] < 30]
    df_pandas['insert_ts'] = datetime.now()
    df_pandas = df_pandas.sort_values(by=['userID'])
    df_pandas = df_pandas.reset_index(drop=True)

    return df_pandas

df = spark.read.csv("Friends.csv", header=True, inferSchema=True)

schema = StructType([StructField("userID", IntegerType()), StructField("name", StringType()), StructField("age", IntegerType()), StructField("friends", IntegerType()), StructField("insert_ts", TimestampType())])

# transform into a spark dataframe to upload it to HDFS
@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def transform_df_to_udf(pdf):
    # apply the transformation
    return transform_df(pdf)

df = df.groupby("userID").apply(transform_df_to_udf)

# write the DataFrame to HDFS
df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/CSV_to_parquet/")\
.partitionBy("age")\
.save()