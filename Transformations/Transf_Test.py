import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, length, count
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("bigData").getOrCreate()
corpus = [''.join(random.choices(string.ascii_lowercase, k=100)) for i in range(1000000)]
df = spark.createDataFrame(corpus, StringType())

df = df.select(explode(split(df.value, "")))

df = df.groupBy('col').agg(count('*').alias('count')).orderBy('count', ascending=False)

df.show(26)