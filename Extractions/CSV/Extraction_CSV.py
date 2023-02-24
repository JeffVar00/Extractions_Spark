from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# SparkSession
spark = SparkSession.builder.appName("CSV to HDFS").master("spark://localhost:7077").getOrCreate()

# read CSV file into a DataFrame
df = spark.read.csv("Friends.csv", header=True, inferSchema=True)

# fill null values with default value
df = df.fillna(value="Unknown", subset=["name"]).fillna(value=30, subset=["age"])
df = df.fillna("N/A")

# transformation example
df = df.select(df.userID,df.name\
         ,df.age,df.friends)\
         .where(df.age < 40 ).withColumn('insert_ts', func.current_timestamp())\
         .orderBy(df.userID).cache()

# debugging
df.show()

# df.createOrReplaceTempView("peoples")
# spark.sql("select userID, name, age from peoples where friends > 100 order by userID").show()

# write the DataFrame to HDFS as parquet
df.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/CSV_to_parquet/")\
.partitionBy("age")\
.save()