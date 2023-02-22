from pyspark.sql import SparkSession
import pandas as pd
import time

# SparkSession
spark = SparkSession.builder.appName("JSON/Pandas to HDFS").getOrCreate()

start = time.time()

# read JSON file into a DataFrame using pandas
df = pd.read_json("Employees.json")

# show the dataframe, the first 10 values
print(df.head(10))

# show if there is any missing values
print(df.isnull().sum())

# replace null values in specific columns with a default value
default_values = {'name' : "Unknown", "department": 'Unspecified'}
df.fillna(value=default_values, inplace=True)

# fill the missing values in the age column with the mean of that column, you can use also Min and Max.
df["age"].fillna(value=df["age"].mean(numeric_only=True), inplace=True)

# fill the rest of the columns with a N/A value
df.fillna(value="N/A", inplace=True)

# show if there is any missing values
print(df.isnull().sum())

end = time.time()
print(" HEYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY " + str(end - start) )

# transform into a spark dataframe to upload it to HDFS
df_spark = spark.createDataFrame(df)

# write the DataFrame to HDFS
df_spark.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/JSON_to_parquet/")\
.partitionBy("department")\
.save()