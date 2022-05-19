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



#### 5) get airport details which is having maximum number of takeoffs and landings
a = routes.groupBy(col('src_airport'), col('src_airport_id')).count() \
    .select('src_airport', 'src_airport_id', col('count').alias('takeoffs'))
b = routes.groupBy(col('dest_airport'), col('dest_airport_id')).count() \
    .select('dest_airport', 'dest_airport_id', col('count').alias('landings'))


joindf = a.join(b, a['src_airport'] == b['dest_airport'], 'full') \
    .select('src_airport', 'dest_airport', (col('takeoffs') + col('landings')).alias('total_traffic')) \
    .orderBy(desc('total_traffic'))


resdf = airport.join(joindf, airport.Iata == joindf.src_airport) \
    .select(airport.Airport_id, airport.Name, joindf.total_traffic) \
    .orderBy(desc('total_traffic'))
resdf.show(1, truncate=False)

# details = portdf.join(a, portdf.Iata == a.src_airport, 'inner').select(
#     [portdf.Airport_id, portdf.Name, portdf.City, a.src_airport, a.dest_airport, a['count']]) \
#     .filter(a['count'] == b)


### ---sql-----------

# airport.createOrReplaceTempView("airport")
# routes.createOrReplaceTempView("routes")
#
# query1 = f"""select name,airport_id,src_airport,dest_airport,frequency from (select ar.name,ar.airport_id,
#                 rt.src_airport,rt.dest_airport,count(*) as frequency,row_number()
#                 over (order by count(*) desc) as r1
#                 from airport ar join routes rt on (ar.Iata = rt.src_airport)
#                 group by ar.name,ar.airport_id,rt.src_airport,rt.dest_airport
#                 order by ar.name) where r1 = 1"""
# spark.sql(query1).show()