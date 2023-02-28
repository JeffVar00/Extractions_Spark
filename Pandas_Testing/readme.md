# pandas version
# fillna methods -> https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.fillna.html
#   pad : fill with previous value
#   bfill : fill with next value
#
# fillna axis -> 
#   0 : fill row-wise
#   1 : fill column-wise
#
# fillna limit ->
#   int : limit of how many values to fill
#   None : fill all values
#
# fillna inplace ->
#   True : fill in place
#   False : fill and return new dataframe

# interpolate methods -> https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.interpolate.html
#   linear : ignore the index and treat the values as equally spaced. This is the only method supported on MultiIndexes.
#   time : interpolation works on daily and higher resolution data to interpolate given length of interval.
#   index, values : use the actual numerical values of the index.
#   pad / ffill : propagate last valid observation forward to next valid.
#   nearest : use nearest valid observations to fill gap.
#   zero : use 0 as the nearest valid observation to fill gap.
#   slinear : same as 'linear', but ignore the index.
#   quadratic : polynomial interpolation of order 2.
#   cubic : polynomial interpolation of order 3.
#   barycentric : weighted averages of values at current and other known points.
#   krogh : cubic interpolation, as in the FITPACK routine.
#   polynomial : polynomial interpolation of specified order.
#   spline : cubic spline interpolation, as in the FITPACK routine.
#   piecewise_polynomial : use the "piecewise polynomial" algorithm of Pawlowicz et al.

# Load SQL from the database in Pandas

# dropna

# axis ->
#   0 or ‘index’ : Drop rows which contain missing values.
#   1 or ‘columns’ : Drop columns which contain missing value.
#
# how ->
#   ‘any’ : If any NA values are present, drop that row or column.
#   ‘all’ : If all values are NA, drop that row or column.

###

<!-- SCALAR: las funciones de usuario definidas por Pandas que operan en una fila de entrada y devuelven una fila de salida. En otras palabras, las funciones de usuario que toman un conjunto de valores de entrada (en forma de una fila de DataFrame) y producen un único valor de salida (en forma de una columna de DataFrame).

SCALAR_ITER: las funciones de usuario definidas por Pandas que operan en un conjunto de filas de entrada y devuelven un conjunto de filas de salida. En otras palabras, las funciones de usuario que toman un conjunto de valores de entrada (en forma de un conjunto de filas de DataFrame) y producen un conjunto de valores de salida (en forma de una o varias columnas de DataFrame).

GROUPED MAP: Se utiliza para aplicar una función de usuario definida por Pandas a cada grupo de filas de un DataFrame de PySpark que comparten una clave común. Es útil para operaciones que implican una agrupación de datos en PySpark, como el cálculo de agregaciones por grupos o el procesamiento de ventanas.

MAP: la función definida por el usuario toma como entrada una columna de un DataFrame de PySpark y devuelve otra columna de PySpark. Se utiliza para aplicar una función de usuario definida por Pandas a una sola columna de un DataFrame de PySpark.

CROSS JOIN: la función definida por el usuario toma dos DataFrames de PySpark y devuelve otro DataFrame de PySpark. Se utiliza para aplicar una función de usuario definida por Pandas a todas las combinaciones posibles de filas de dos DataFrames de PySpark.

GROUPED AGG: la función definida por el usuario toma como entrada un DataFrame de PySpark agrupado y devuelve otro DataFrame de PySpark. Se utiliza para aplicar una función de usuario definida por Pandas a cada grupo de filas de un DataFrame de PySpark y devolver un DataFrame de PySpark que representa el resultado de aplicar la transformación a todos los grupos. -->

###

<!-- username='jeff'
password='1234'
host='172.17.80.1'
port='1433'
database= 'AdventureWorks2019'

url = 'mssql+pyodbc://{user}:{passwd}@{host}:{port}/{db}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'.format(user=username, passwd=password, host=host, port=port, db=database)

# establishing the connection to the database using engine as an interface
engine = create_engine(url)

with engine.begin() as connection: 
    df = pd.read_sql(text('SELECT * FROM Person.Address'), connection)


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
df = pd.read_sql(query, cnxn) -->