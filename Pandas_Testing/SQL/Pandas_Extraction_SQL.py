from pyspark.sql import SparkSession
import pyodbc
import pandas as pd
import time
#from sqlalchemy import create_engine, text

spark = SparkSession.builder.appName("SQL/Pandas to HDFS").getOrCreate()

start = time.time()

"""
username='jeff'
password='1234'
host='172.17.80.1'
port='1433'
database= 'AdventureWorks2019'

url = 'mssql+pyodbc://{user}:{passwd}@{host}:{port}/{db}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'.format(user=username, passwd=password, host=host, port=port, db=database)

# establishing the connection to the database using engine as an interface
engine = create_engine(url)

with engine.begin() as connection: 
    df = pd.read_sql(text('SELECT * FROM Person.Address'), connection)
"""

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = '172.17.80.1,1433' 
database = 'AdventureWorks2019' 
username = 'jeff' 
password = '1234'  
cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password + ';TrustServerCertificate=yes;')
cursor = cnxn.cursor()


query = "SELECT AddressID, AddressLine1, AddressLine2, City, StateProvinceID, PostalCode, ModifiedDate FROM Person.Address;"
df = pd.read_sql(query, cnxn)

# show the dataframe, the first 10 values
print(df.head(10))

# show if there is any missing values
print(df.isnull().sum())

# pandas version
df.dropna(how='any', inplace=True)

# axis ->
#   0 or ‘index’ : Drop rows which contain missing values.
#   1 or ‘columns’ : Drop columns which contain missing value.
#
# how ->
#   ‘any’ : If any NA values are present, drop that row or column.
#   ‘all’ : If all values are NA, drop that row or column.

print(df.isnull().sum())

end = time.time()
print(" HEYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY " + str(end - start) )

df_spark = spark.createDataFrame(df)

df_spark.write\
.format("parquet").mode("overwrite")\
.option("path", "hdfs://localhost:9000/user/hadoop_ADMIN/spark/SQL_to_parquet/")\
.partitionBy("City")\
.save()