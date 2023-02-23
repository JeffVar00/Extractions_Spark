from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

# SparkSession
spark = SparkSession.builder.appName("JSON/Pandas to HDFS").master("spark://localhost:7077").getOrCreate()

# read JSON file into a DataFrame using pandas
# df = pd.read_json("Employees.json")

def transform_df(df):

    print(df.head(10))
    print(df.isnull().sum())

    # replace null values in specific columns with a default value
    default_values = {'name' : "Unknown", "department": 'Unspecified'}
    df.fillna(value=default_values, inplace=True)

    # fill the missing values in the age column with the mean of that column, you can use also Min and Max.
    df["age"].fillna(value=df["age"].mean(numeric_only=True), inplace=True)

    # fill the rest of the columns with a N/A value
    df.fillna(value="N/A", inplace=True)

    print(df.isnull().sum())

    return df

df = spark.read.option("inferSchema", "true").option("header", "true").option("multiline","true").json("Employees.json")

@pandas_udf(df.schema, functionType=PandasUDFType.GROUPED_MAP)
def transform_df_to_udf(pdf):
    return transform_df(pdf)

df = df.apply(transform_df_to_udf)

df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/JSON_to_parquet/")\
.partitionBy("department")\
.save()