from pyspark.sql import SparkSession
from sparksessionutils import sparksessionutils
from pyspark.sql.functions import *
from pyspark.sql.types import *



##----------Mayuresh Sonawane-------------##
if __name__ == "__main__":

    spark = SparkSession.builder.master("local[*]").appName("airlinemgmt")\
            .getOrCreate()


    rdf = sparksessionutils()


    airlineschema = StructType([StructField("airline_id", IntegerType()),
                            StructField("name", StringType()),
                            StructField("alias", StringType()),
                            StructField("iata", StringType()),
                            StructField("icao", StringType()),
                            StructField("callsign", StringType()),
                            StructField("country", StringType()),
                            StructField("active", StringType())])
    airdf = rdf.readCsv(spark=spark,path=r"D:\airline data\airline.csv",schema=airlineschema,header=True)
    # airdf.printSchema()
    print("--------------airline---------------")
    airdf.show()

    airportschema = StructType([StructField("Airport_id", IntegerType()),
                            StructField("Name", StringType()),
                            StructField("City", StringType()),
                            StructField("Country", StringType()),
                            StructField("Iata", StringType()),
                            StructField("Icao", StringType()),
                            StructField("Null", IntegerType()),
                            StructField("Latitude", IntegerType()),
                            StructField("Longitude", IntegerType()),
                            StructField("Altitude", IntegerType()),
                            StructField("Timezone", StringType()),
                            StructField("DST", StringType()),
                            StructField("Tz", StringType()),
                            StructField("Type", StringType()),
                            StructField("Source", StringType())])

    portdf = spark.read.csv(r"D:\airline data\airport.csv", schema=airportschema, header=True)
    # portdf.printSchema()
    print("--------airport-------------")
    # portdf.show()


    plane = spark.read.csv(r"D:\airline data\plane.csv",inferSchema=True,sep='',header=True)
    plane.printSchema()
    print("--------planes-------------")
    plane.show()


    routes  = spark.read.parquet(r"D:\airline data\routes.snappy.parquet")
    # routes.printSchema()
    print("--------routes-------------")
    routes.show()

#### 1) in any of your input file if you are getting \N or null values in your column and that column is of string type
# then put default value as "Uncommon" and if column is of  type integer put -1


    df= airdf.fillna('(unknown)',['iata','callsign','icao','country'])
    df1 = df.withColumn('alias',regexp_replace(df['alias'],r'\\N','(unknown)')).show(truncate=False)

#--------for airport null and \N----------------

    portnull = portdf.fillna('(unknown)',['null','source']).na.fill(value=-1)

    portnull.withColumn('Altitude',when(portnull['Altitude'] == 0 ,-1).otherwise(portnull["Altitude"])).show()

#-------for planes-----------------

    # planenull = plane.select('*').replace(r'\N',value='(unknown)',subset=['ICAO code'])
    planenull = plane.withColumn("ICAO code", regexp_replace(plane["ICAO code"], r"\\N", "(unknown)"))
    planenull.show()




#### 2) find the country name which is having both airlines and airport

#---1st method

    output = airdf.join(portdf,airdf.airline_id == portdf.Airport_id).filter(airdf.country == portdf.Country)\
         .select(airdf.airline_id,portdf.Airport_id,airdf.country)
    # output.show()

#2nd method

    airdf.join(portdf,airdf.airline_id == portdf.Airport_id).filter(airdf.country == portdf.Country)\
     .select([airdf.name,airdf.country,portdf.Name,portdf.Country]).show()

# 3rd method-----------SQL--------
    portdf.createOrReplaceTempView("airport")
    airdf.createOrReplaceTempView("airline")
    # spark.sql("select * from airport").show()
    # spark.sql("select * from airline").show()

    spark.sql("select a.airline_id,a.name,a.country,b.Airport_id,b.Name,b.Country from airline a join airport b on "
          "a.country = b.Country").show()




## 3) get the airlines details like name,id,which is has been taken takeoff more than 3 times from same airport


    takeoff = routes.join(airdf, routes.airline_id == airdf.airline_id, "inner").groupBy(airdf.airline_id, airdf.name ,routes.src_airport_id,routes.src_airport).count()
    takeoff.where(col("count") > 3).distinct().show()


## 3) get airport details which has minimum number of takeoffs and landings

    takeoff = routes.join(portdf,routes.src_airport == portdf.Iata,'inner').select([portdf.Airport_id,portdf.Name,portdf.City,routes.src_airport,routes.dest_airport])\
    .groupBy(portdf['Airport_id'],portdf['Name'],portdf['City'],routes['src_airport'],routes['dest_airport']).count()
    takeoff.where(col("count") <= 1).distinct().show()

#### 4) get airport details which is having maximum number of takeoffs and landings

    a = routes.groupBy(col('src_airport'),col('dest_airport')).count()
    b = routes.groupBy(col('src_airport'),col('dest_airport')).count().orderBy(desc('count')).select('count').head(1)[0]['count']
    # a.show()

    details = portdf.join(a, portdf.Iata == a.src_airport,'inner').select([portdf.Airport_id,portdf.Name,portdf.City,a.src_airport,a.dest_airport,a['count']])\
        .filter(a['count'] >= b).show()


##### 5) Get the airline details which is having direct flights. details like airline id,name,source airport name,destination airport name


    a = routes.filter(routes['stops'] == 0)
    # a.show()

    airdet = airdf.join(a, airdf.airline_id == a.airline_id,'inner')\
        .select(airdf.airline_id,airdf.name,a.src_airport,a.dest_airport)
    airdet.show()