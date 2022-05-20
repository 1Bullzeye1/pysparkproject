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


a = routes.groupBy(col('src_airport'), col('src_airport_id')).count() \
    .select('src_airport', 'src_airport_id', col('count').alias('takeoffs'))
b = routes.groupBy(col('dest_airport'), col('dest_airport_id')).count() \
    .select('dest_airport', 'dest_airport_id', col('count').alias('landings'))

joindf = a.join(b, a['src_airport'] == b['dest_airport'], 'full') \
    .select('src_airport', 'dest_airport', (col('takeoffs') + col('landings')).alias('total_traffic')) \
    .orderBy(desc('total_traffic'))
resdf = airport.join(joindf, airport.Iata == joindf.src_airport) \
    .select(airport.Airport_id, airport.Name, joindf.total_traffic) \
    .orderBy(asc('total_traffic'))

resdf1 = resdf.filter('total_traffic is not null')

resdf1.show()
print(resdf1.count())

details = airport.join(a, airport.Iata == a.src_airport, 'inner').select(
    [airport.Airport_id, airport.Name, airport.City, a.src_airport, a.dest_airport, a['count']]) \
    .filter(a['count'] == b)
details.show()
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