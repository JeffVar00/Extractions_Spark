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

