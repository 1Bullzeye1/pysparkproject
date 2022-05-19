import os,sys
from pyspark.sql import *
from pyspark.sql.functions import struct
from pyspark.sql.types import *
from airlinedf import *
from airportdf import *
from planesdf import *
from routesdf import *


#### 1) in any of your input file if you are getting \N or null values in your column and that column is of string type
# then put default value as "Uncommon" and if column is of  type integer put -1

df1 = airlinedf()
df2 = airportdf()
df3 = planesdf()
df4 = routesdf()

airline = df1.airlines()
airport = df2.airports()
planes = df3.planes()
routes = df4.routes()
# ----------------airline----------------------#

dtf = airline.fillna('(unknown)', ['iata', 'callsign', 'icao', 'country'])
dtf = dtf.withColumn('alias', regexp_replace(dtf['alias'], r'\\N', '(unknown)'))
print("#--------------for airline----------#")
dtf.show()
# df1.write.csv(r"output/csvairdf")

# _-------------sql--------------------#

airline.createOrReplaceTempView("airline")
#spark.sql("")

# --------airport--------------------#
airport.printSchema()
portnull = airport.fillna('(unknown)').na.fill(value=-1)
print(r"# --------for airport null and \N--------------#")
portnull.show()
# portnull.withColumn('Altitude', when(portnull['Altitude'] == 0, -1)\
#                     .otherwise(portnull["Altitude"])).show()

print(r"#-------for planes---------------#")

planenull = planes.select('*').replace(r'\N',value='(unknown)',subset=['ICAO code'])
planedf = planenull.withColumn("ICAO code", regexp_replace(planes["ICAO code"], r"\\N", "(unknown)"))
planedf.show()


routenull = routes.fillna('(unknown)','codeshare')\
           .withColumn("dest_airport_id",regexp_replace(routes["dest_airport_id"],r"\\N", "(unknown)")) \
           .withColumn("src_airport_id", regexp_replace(routes["src_airport_id"], r"\\N", "(unknown)"))\
           .withColumn("airline_id", regexp_replace(routes["airline_id"], r"\\N", "(unknown)"))
print(r"#----------for routes-------------#")
routenull.show()

