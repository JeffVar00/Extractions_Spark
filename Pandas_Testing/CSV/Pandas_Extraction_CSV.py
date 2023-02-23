from pyspark.sql import SparkSession
import pandas as pd

# SparkSession
spark = SparkSession.builder.appName("CSV/Pandas to HDFS").getOrCreate().master("spark://localhost:7077")

# read JSON file into a DataFrame using pandas
df = pd.read_csv("Friends.csv")

# show the dataframe, the first 10 values
print(df.head(20))

# show if there is any missing values
print(df.isnull().sum())

# replace null values in specific columns with a default value
default_values = {'name' : "Unknown"}
df.fillna(value=default_values, inplace=True)

# interpolate the age column
df["age"] = df["age"].interpolate(method="linear")

# fill the rest of the columns with a N/A value
df.fillna(value="N/A", inplace=True)

# show if there is any missing values
print(df.isnull().sum())

# transform into a spark dataframe to upload it to HDFS
df_spark = spark.createDataFrame(df)

# write the DataFrame to HDFS
df_spark.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/CSV_to_parquet/")\
.partitionBy("age")\
.save()