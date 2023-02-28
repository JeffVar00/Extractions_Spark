from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL to HDFS").master("spark://localhost:7077").getOrCreate()

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

# show the dataframe for the pyspark df
df.printSchema()
df.show(10)

# create a column with the number of nulls in each row
# null_cols = [df[col].isNull().alias(col + "_null") for col in df.columns]

# create a dataframe with the number of nulls in each column
# null_counts = df.select(null_cols).agg(*[sum(col).alias(col + "_null_count") for col in df.columns])

# show the dataframe with the number of nulls in each column
# null_counts.select([col for col in null_counts.columns if 'null_count' in col]).show()

# pyspark version for dropping columns with nulls
df = df.na.drop(how='any')

df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/SQL_to_parquet/")\
.partitionBy("City")\
.save()