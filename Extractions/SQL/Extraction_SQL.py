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

# pyspark version for dropping columns with nulls
df = df.na.drop(how='any')

df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/SQL_to_parquet/")\
.partitionBy("City")\
.save()