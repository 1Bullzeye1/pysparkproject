from pyspark.sql import *
from sparksessionutils import sparksessionutils
from pyspark.sql.functions import *
from pyspark.sql.types import *

##----------Mayuresh Sonawane-------------##
if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[*]")\
        .config("spark.driver.memory","6g").appName("airlinemgmt") \
        .getOrCreate()

    rdf = sparksessionutils()
#-------------airline schema--------------#
    airlineschema = StructType([StructField("airline_id", IntegerType()),
                                StructField("name", StringType()),
                                StructField("alias", StringType()),
                                StructField("iata", StringType()),
                                StructField("icao", StringType()),
                                StructField("callsign", StringType()),
                                StructField("country", StringType()),
                                StructField("active", StringType())])
#----------------Dataframe----------------------#

    airdf = rdf.readCsv(spark=spark, path=r"input/airline.csv", schema=airlineschema, header=True)
    # airdf.printSchema()
    print("--------------airline---------------")
    airdf.show()





#-------------airport Schema---------------#

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

    portdf = spark.read.csv(r"input/airport.csv", schema=airportschema, header=True)
    # portdf.printSchema()
    print("--------airport-------------")
    portdf.show()

    plane = spark.read.csv(r"input/plane.csv", inferSchema=True, sep='', header=True)
    plane.printSchema()
    print("--------planes-------------")
    plane.show()

    routes = spark.read.parquet(r"input/routes.snappy.parquet")
    routes.printSchema()
    print("--------routes-------------")
    routes.show()


    #### 1) in any of your input file if you are getting \N or null values in your column and that column is of string type then put default value as "Uncommon" and if column is of  type integer put -1

    # df = airdf.fillna('(unknown)', ['iata', 'callsign', 'icao', 'country'])
    # df1 = df.withColumn('alias', regexp_replace(df['alias'], r'\\N', '(unknown)'))
    # df1.write.csv(r"output/csvairdf")

    # _-------------sql--------------------#

    airdf.createOrReplaceTempView("airline")
    # spark.sql("")


    # --------for airport null and \N----------------

    portnull = portdf.fillna('(unknown)', ['null', 'source']).na.fill(value=-1)

    portnull.withColumn('Altitude', when(portnull['Altitude'] == 0, -1).otherwise(portnull["Altitude"])).show()

    # -------for planes-----------------

    # planenull = plane.select('*').replace(r'\N',value='(unknown)',subset=['ICAO code'])
    planenull = plane.withColumn("ICAO code", regexp_replace(plane["ICAO code"], r"\\N", "(unknown)"))
    planenull.show()

    #----------for routes-------------
    routenull = routes.fillna('(unknown)','codeshare')\
        .withColumn("dest_airport_id",regexp_replace(routes["dest_airport_id"],r"\\N", "(unknown)"))
    routenull.show()


    #### 2) find the country name which is having both airlines and airport

    # ---1st method

    output = airdf.join(portdf, airdf.airline_id == portdf.Airport_id).filter(airdf.country == portdf.Country) \
        .select(airdf.airline_id, portdf.Airport_id, airdf.country)
    output.show()


    # 2nd method-----------SQL--------
    portdf.createOrReplaceTempView("airport")
    airdf.createOrReplaceTempView("airline")
    # spark.sql("select * from airport").show()
    # spark.sql("select * from airline").show()
    print("----------sql---------------- \n")
    sqldf = spark.sql("select ar.Country as Country,al.name as airline_name,ar.Name as airport_name from "
              "airline al join airport ar on (al.country = ar.Country)")

    sqldf.show()
    sqldf.printSchema()
    print("\n")

    sqldf.createOrReplaceTempView("planeinfo")

    # spark.sql("select Country,LISTAGG(airline_name,', ') WITHIN GROUP (order by airline_name ASC) AS Airline_name from planeinfo group by Country").show()

    # spark.sql("select Country,LISTAGG(airline_name, ', ') from planinfo "
    #           "group by Country "
    #           "order by Country;").show()

    # airpdf = sqldf.groupBy("Country").agg(collect_list("airport_name").alias("Airport Name")).orderBy("Country")
    # airpdf.show()
    # airldf = sqldf.groupBy("Country").agg(collect_list("airline_name").alias("Airline Name")).orderBy("Country")
    # airldf.show()
    airpdf = sqldf.groupBy("Country").agg(concat_ws(", ",collect_list("airport_name"))\
                                          .alias("Airport_Name"))
    # airpdf.show()

    airldf = sqldf.groupBy("Country").agg(concat_ws(", ",collect_list("airline_name"))\
                                          .alias("Airline_Name"))
    # airldf.show()

    # sqldf.groupBy("Country").agg(concat_ws(", ", collect_list("airport_name")).alias("airports")).show()


    joidf = airpdf.join(airldf,airpdf.Country == airldf.Country,"inner")\
        .select(airpdf["Country"], "Airport_Name", "Airline_Name")
    # joidf.show()

    # joidf.write.csv(r"output/countairport",header=True)
    # joidf.write.json(r"output/jsondf")

    csvdf = spark.read.csv(r"output/countairport/",header=True)
    csvdf.show()

    # sqldf.agg(concat_ws(", ", expr("sort_array(collect_list(codeDate)).CODE")).alias("CODE")).show()
    #
    # sqldf.printSchema()

    # spark.sql(
    #     "select a.airline_id,a.name,a.country,b.Airport_id,b.Name,b.Country from airline a join airport b on "
    #     "a.country = b.Country").show()

    # ## 3) get the airlines details like name,id,which is has been taken takeoff more than 3 times from same airport
    #
    # takeoff = routes.join(airdf, routes.airline_id == airdf.airline_id, "inner").groupBy(
    #     airdf.airline_id, airdf.name,
    #     routes.src_airport_id,
    #     routes.src_airport).count()
    # takeoff.where(col("count") > 3).distinct().show()
    #
    # ## 3) get airport details which has minimum number of takeoffs and landings
    #
    #
    # takeoff = routes.join(portdf, routes.src_airport == portdf.Iata, 'inner').select(
    #     [portdf.Airport_id, portdf.Name, portdf.City, routes.src_airport, routes.dest_airport]) \
    #     .groupBy(
    #     portdf['Airport_id'], portdf['Name'], portdf['City'], routes['src_airport'],
    #     routes['dest_airport']).count()
    # print(takeoff.where(col("count") == 1).count())
    #
    # #### 4) get airport details which is having maximum number of takeoffs and landings
    #
    # a = routes.groupBy(col('src_airport'), col('dest_airport')).count()
    # b = \
    #     routes.groupBy(col('src_airport'), col('dest_airport')).count().orderBy(desc('count')).select('count').head(1)[
    #         0][
    #         'count']
    # # a.show()
    #
    # details = portdf.join(a, portdf.Iata == a.src_airport, 'inner').select(
    #     [portdf.Airport_id, portdf.Name, portdf.City, a.src_airport, a.dest_airport, a['count']]) \
    #     .filter(a['count'] >= b).show()
    #
    # ##### 5) Get the airline details which is having direct flights. details like airline id,name,source airport name,destination airport name
    #
    # a = routes.filter(routes['stops'] == 0)
    # # a.show()
    #
    # airdet = airdf.join(a, airdf.airline_id == a.airline_id, 'inner') \
    #     .select(airdf.airline_id, airdf.name, a.src_airport, a.dest_airport)
    # airdet.show()
    #
    # print(airdet.count())
