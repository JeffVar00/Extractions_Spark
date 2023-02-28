from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

# SparkSession
spark = SparkSession.builder.appName("JSON/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

# read JSON file into a DataFrame using pandas
# df = pd.read_json("Employees.json")

df = spark.read.option("inferSchema", "true").option("header", "true").option("multiline","true").json("Employees.json")

@pandas_udf("integer", PandasUDFType.SCALAR)
def fill_nulls_with_mean_age(age: pd.Series) -> pd.Series:

    # Compute the mean age excluding null values
    mean_age = age.mean(skipna=True)
    # Fill null values with the mean age
    age.fillna(mean_age, inplace=True)

    return age

@pandas_udf(df.schema, functionType=PandasUDFType.GROUPED_MAP)
def fill_nulls(df: pd.DataFrame) -> pd.DataFrame:

    # replace null values in specific columns with a default value
    default_values = {'name' : "Unknown", "department": 'Unspecified'}
    df.fillna(value=default_values, inplace=True)

    # fill the rest of the columns with a N/A value
    df.fillna(value="N/A", inplace=True)

    return df

df = df.withColumn('age', fill_nulls_with_mean_age(df['age']))
df = df.groupby().apply(fill_nulls)

df.show()

df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/JSON_to_parquet/")\
.partitionBy("department")\
.save()