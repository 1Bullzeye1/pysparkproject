from sparksessionutils import *
from pyspark.sql import *
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





# 4) get airport details which has minimum number of takeoffs and landings


a = routes.groupBy(col('src_airport'), col('dest_airport')).count()
b = routes.groupBy(col('src_airport'), col('dest_airport')).count().orderBy(asc('count')).select('count').head(1)[
    0]['count']

details = airport.join(a, airport.Iata == a.src_airport, 'inner').select(
    [airport.Airport_id, airport.Name, airport.City, a.src_airport, a.dest_airport, a['count']]) \
    .filter(a['count'] == b)

print(details.count())

#----sql-----

airline.createOrReplaceTempView("airline")
airport.createOrReplaceTempView("airport")
routes.createOrReplaceTempView("routes")


query = f"""(select ar.name,ar.airport_id,rt.src_airport,rt.dest_airport,count(*) from airport ar
                join routes rt on (ar.Iata = rt.src_airport)
                group by ar.name,ar.airport_id,rt.src_airport,rt.dest_airport)
                 order by count(*) asc"""
query1 = f"""select name,airport_id,src_airport,dest_airport,counts from (select ar.name,ar.airport_id,
                rt.src_airport,rt.dest_airport,count(*) as counts,row_number()
                over (order by count(*) asc) as r1
                from airport ar join routes rt on (ar.Iata = rt.src_airport)
                group by ar.name,ar.airport_id,rt.src_airport,rt.dest_airport
                order by ar.name)"""
print(spark.sql(query).count())
print(spark.sql(query1).count())