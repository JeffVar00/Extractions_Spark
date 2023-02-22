# pyspark
# fillna pyspark -> https://spark.apache.org/docs/latest/api/python/reference/api/pandas.DataFrame.fillna.html
#   pad : fill with previous value
#   bfill : fill with next value
#
# fillna axis -> 
#   0 : fill row-wise
#   1 : fill column-wise
#
# {"city": "unknown", "type": ""}

# pyspark version
# df.na.drop(how='any', thresh=None, subset=None)

# dropna().show(truncate=False) -> another way
#   Return a new DataFrame omitting rows with null values.
# truncate ->
#   If set to True, truncate strings longer than 20 chars by default. If set to a number greater than one, truncates long strings to length truncate and align cells right.

# how ->
#   ‘any’ : If any NA values are present, drop that row or column.
#   ‘all’ : If all values are NA, drop that row or column.
#
# thresh -> int, default None
#   If specified, drop rows that have less than thresh non-NA values. This overwrites the how parameter.
#
# subset -> optional list of column names to consider.
