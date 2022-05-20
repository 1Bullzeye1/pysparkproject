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

a.show()
b.show()

joindf = a.join(b, a['src_airport'] == b['dest_airport'], 'full') \
    .select('src_airport', 'dest_airport', (col('takeoffs') + col('landings')).alias('total_traffic')) \
    .orderBy(desc('total_traffic'))

# joindf.show()

resdf = airport.join(joindf, airport.Iata == joindf.src_airport) \
    .select(airport.Airport_id, airport.Name,joindf.src_airport,joindf.total_traffic) \
    .orderBy(desc('total_traffic'))\
    .filter('total_traffic is not null')
resdf.show(1, truncate=False)

# details = portdf.join(a, portdf.Iata == a.src_airport, 'inner').select(
#     [portdf.Airport_id, portdf.Name, portdf.City, a.src_airport, a.dest_airport, a['count']]) \
#     .filter(a['count'] == b)


### ---sql-----------

airport.createOrReplaceTempView("airport")
routes.createOrReplaceTempView("routes")

query = spark.sql("""select * from airport c where c.iata = 
                    (select a.src_airport,sum(takeoff),row_number()
                     over (order by sum(takeoff) desc) as rowNum from (select 
                     src_airport,count(*) as takeoff from routes group by src_airport union
                     select dest_airport,count(*) from routes group by dest_airport) a group by a.src_airport
                     b where rowNum = 1)""")

query.show()
