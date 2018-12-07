# My experiment on GeoMesa

GeoMesa is a framework that is developed based on spark to do spacial analytics. 

> The Spark JTS module provides a set of User Defined Functions (UDFs) and User Defined Types (UDTs) 
that enable executing SQL queries in spark that perform geospatial operations on geospatial data types.


what is good?

There exists some transformation inside the column which we can not utilize the built-in post-gis 
functionality. (such as st_makePoint(column) which require a column to have a form like Point(x,y) where 
in most case we only have (x,y) as raw data which we call it raw and unstructured data). Therefore, mostly
we might want to do some kind of transformation when parsing data row by by. This is a good situation 
for spark to handle.


 