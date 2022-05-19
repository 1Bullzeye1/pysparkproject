from airlineproject import *

from airlinedf import airlinedf
from airportdf import airportdf
from planesdf import planesdf
from routesdf import routesdf
df1 = airlinedf()
df2 = airportdf()
df3 = planesdf()
df4 = routesdf()


airline = df1.airlines()
airport = df2.airports()
planes = df3.planes()
routes = df4.routes()

## 3) get the airlines details like name,id,which is has been taken takeoff more than 3 times from same airport
takeoff = routes.join(airline, routes.airline_id == airline.airline_id, "inner")\
    .join(airport,routes.src_airport == airport.Iata).groupBy(airline.airline_id, airline.name,routes.src_airport_id,routes.src_airport).count()
res = takeoff.where(col("count") > 3).distinct()

print(res.count())
res.show()

# -----sql-----
#
routes.createOrReplaceTempView("routedf")
airline.createOrReplaceTempView("airlinedf")
airport.createOrReplaceTempView("airportdf")
# spark.sql("select r.airline_id,a.name,r.src_airport_id,r.")


# --- 2nd method--------

filter = routes.select(col('src_airport_id'), col('airline_id').cast('int').alias("airline_id"),
                          col('src_airport')) \
        .groupBy('src_airport_id', 'src_airport', 'airline_id').count()\
        .where(r"src_airport_id != '\N' ")\
        .where('count > 3')
# filter.show()
# filter.printSchema()
condition1 = [filter[ 'airline_id' ]  == airline['airline_id']  ]
condition2 = [filter[ 'src_airport' ]  == airport['Iata']  ]

Result = filter.join(airline, on=condition1) \
        .select(airline.name.alias('Airline_name'),
        airline.airline_id,
        filter['src_airport']) \
        .join(airport, on=condition2) \
        .select('Airline_name',airline.airline_id,
        airport.Name.alias('Airport_Name')).distinct()
print(Result.count())
Result.show()