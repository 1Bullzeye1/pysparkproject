from sparksessionutils import *
from airlineproject import *
from airlinedf import *
from airportdf import *
from planesdf import *
from routesdf import *


df1 = airlinedf()
df2 = airportdf()
df3 = planesdf()
df4 = routesdf()

airline = df1.airlines()
airport = df2.airports()
planes =  df3.planes()
routes = df4.routes()

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

f1 = df.join(c, df.src_airport_id == c.src_airport_id,'inner').distinct()
f2 = f1.join(d, d.dest_airport_id == f1.dest_airport_id,'inner')\
        .select('airline_id','name','source_airport','destination_airport').distinct()

f2.show()

# f1.groupBy().count().show()

# f2 = f1.join(d,df.dest_airport == d.dest_airport,'inner')\
#         .select(df.airline_id,df.name,c.source_airport,d.destination_airport)\
#         .orderBy(asc(df['airline_id']))

# print(final.count())
