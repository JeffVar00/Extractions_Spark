from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import pandas as pd
from datetime import datetime

# SparkSession
spark = SparkSession.builder.appName("CSV/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

df = spark.read.csv("Friends.csv", header=True, inferSchema=True)

schema = StructType([StructField("userID", IntegerType()), StructField("name", StringType()), StructField("age", IntegerType()), StructField("friends", IntegerType()), StructField("date_time", TimestampType())])

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def fill_nulls(df: pd.DataFrame) -> pd.DataFrame:

    # replace null values in specific columns with a default value
    default_values = {'name' : "Unknown"}
    df.fillna(default_values, inplace=True)
    df.fillna("N/A", inplace=True)

    # filering the data
    df = df[df['age'] < 40]
    df['date_time'] = datetime.now()
    df.sort_values(by=['userID'])

    return df

@pandas_udf("integer", PandasUDFType.SCALAR)
def interpolate_age(age: pd.Series) -> pd.Series:
    age = pd.to_numeric(age, errors='coerce')
    return age.interpolate(method='linear')

# SCALAR_ITER : A scalar iterator is a Python generator that takes one row at a time and returns one row at a time.
# SCALAR : A scalar function is a Python function that takes one row as input and returns one row as output.
# GROUPED_MAP : A grouped map function is a Python function that takes one group at a time and returns one row at a time.
# GROUPED_AGG : A grouped aggregate function is a Python function that takes one group at a time and returns one aggregated value at a time.

df = df.withColumn('age', interpolate_age(df['age']))
df = df.groupby().apply(fill_nulls)
df.show()

# write the DataFrame to HDFS
df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/CSV_to_parquet/")\
.partitionBy("age")\
.save()