from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# SparkSession
spark = SparkSession.builder.appName("CSV to HDFS").getOrCreate()

# read CSV file into a DataFrame
df = spark.read.csv("Friends.csv", header=True, inferSchema=True)

# show the DataFrame
df.printSchema()
df.show()

# fill null values with default value
df = df.fillna(value="Unknown", subset=["name"]).fillna(value=30, subset=["age"])
df = df.fillna("N/A").show()

# transformation example
output = df.select(df.userID,df.name\
         ,df.age,df.friends)\
         .where(df.age < 30 ).withColumn('insert_ts', func.current_timestamp())\
         .orderBy(df.userID).cache()

# show the DataFrame
output.createOrReplaceTempView("peoples")
spark.sql("select userID, name from peoples where friends > 100 order by userID").show()

# write the DataFrame to HDFS as parquet
df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/CSV_to_parquet/")\
.partitionBy("age")\
.save()