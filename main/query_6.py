from sparksessionutils import *
from airlineproject import *
from airlinedf import *
from airportdf import *
from planesdf import *
from routesdf import *


df1 = airlinedf()
df2 = airportdf()
df3 = routesdf()
spark = df1.sparks()
airline = df1.airlines()
airport = df2.airports()
routes = df3.routes()

##### 6) Get the airline details which is having direct flights. details like airline id,name,source airport
    # name,destination airport name

print(routes.count())

routesf = routes.filter(routes['stops'] == 0)
airlinef = airline.filter(airline['active'] == 'Y')

df = routesf.join(airlinef, routesf.airline_id == airlinef.airline_id) \
    .select(airlinef.airline_id, airlinef.name, routesf.src_airport_id, routes.dest_airport_id)
print(df.count())
df.printSchema()

c = routesf.join(airport, airport.Airport_id == routesf.src_airport_id) \
    .select(routes.src_airport_id, airport['name'].alias('source_airport'))
d = routesf.join(airport, airport.Airport_id == routesf.dest_airport_id) \
    .select(routes.dest_airport_id, airport['name'].alias('destination_airport'))

print(c.count())
print(d.count())

f1 = df.join(c, df.src_airport_id == c.src_airport_id,'inner').distinct()\
     .join(d, df.dest_airport_id == c.dest_airport_id,'inner')\
     .select('airline_id','name','source_airport','destination_airport').distinct()

f1.show()
print(f1.count())

# -----------sql-------------

routes.createOrReplaceTempView("routes")
airline.createOrReplaceTempView("airline")
airport.createOrReplaceTempView("airport")

query = spark.sql(""" select a.airline_id,a.name airline_name,ap.name source_airport,
                   ap1.name destination_airport from routes r
                   join airline a on r.airline_id = ap.airline_id
                   left join airport ap on r.src_airport_id = ap.airport_id
                   left join airport ap1 on r.des_airport_id = ap1.airport_id
                   where stops = 0 order by (a.airline_id) asc""")

query.show()