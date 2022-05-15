from pyspark.sql import *
from sparksessionutils import sparksessionutils
from pyspark.sql.functions import *
from pyspark.sql.types import *



##----------Mayuresh Sonawane-------------##
if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[*]")\
        .config("spark.driver.memory","8g")\
        .appName("airlinemgmt") \
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
    airdf.printSchema()
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
    portdf.printSchema()
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

    # portnull.withColumn('Altitude', when(portnull['Altitude'] == 0, -1)\
    #                     .otherwise(portnull["Altitude"])).show()

    # -------for planes-----------------

    # planenull = plane.select('*').replace(r'\N',value='(unknown)',subset=['ICAO code'])
    planenull = plane.withColumn("ICAO code", regexp_replace(plane["ICAO code"], r"\\N", "(unknown)"))
    # planenull.show()

    #----------for routes-------------
    routenull = routes.fillna('(unknown)','codeshare')\
        .withColumn("dest_airport_id",regexp_replace(routes["dest_airport_id"],r"\\N", "(unknown)")) \
        .withColumn("src_airport_id", regexp_replace(routes["src_airport_id"], r"\\N", "(unknown)"))\
        .withColumn("airline_id", regexp_replace(routes["airline_id"], r"\\N", "(unknown)"))
    # routenull.show(1000)


    #### 2) find the country name which is having both airlines and airport

    # ---1st method

    output = airdf.join(portdf, airdf.country == portdf.Country,"inner") \
            .select(airdf.country.alias("Country"), portdf.Name.alias("Airport_name"), airdf.name.alias("Airline_name"))

    # output.show()
    # output.printSchema()

    # ---2nd type

    airpdf = output.groupBy("Country").agg(
        concat_ws(", ", collect_list("Airport_name")) \
        .alias("Airport_Name"))
    # airpdf.show()

    airldf = output.groupBy("Country").agg(
        concat_ws(", ", collect_list("Airline_name")) \
        .alias("Airline_Name"))
    # airldf.show()

    joidf = airpdf.join(airldf, airpdf.Country == airldf.Country, "inner") \
        .select(airpdf["Country"], "Airport_Name", "Airline_Name")
    joidf.show(truncate=False)


    # joidf.write.csv(r"output/countairport",header=True)
    # joidf.write.json(r"output/jsondf")

    # csvdf = spark.read.csv(r"output/countairport/", header=True)
    # csvdf.show()


    # # 3rd method-----------SQL--------
    portdf.createOrReplaceTempView("airport")
    airdf.createOrReplaceTempView("airline")
    # # spark.sql("select * from airport").show()
    # # spark.sql("select * from airline").show()
    # print("----------sql---------------- \n")
    # sqldf = spark.sql("select ar.Country as Country,al.name as Airline_name,ar.Name as Airport_name from airline al "
    #                   "join airport ar on (al.country = ar.Country)")
    #
    # # sqldf.createOrReplaceTempView("sqldf")
    # query = f"""SELECT Country ,
    #         LISTAGG(Airline_name, '; ') WITHIN GROUP (ORDER BY Country) "Airlines"
    #         FROM sqldf GROUP BY department_id ORDER BY department_id;"""
    # spark.sql(query).show()
    #
    # sqldf.printSchema()
    # print("\n")


    ## 3) get the airlines details like name,id,which is has been taken takeoff more than 3 times from same airport

    # takeoff = routes.join(airdf, routes.airline_id == airdf.airline_id, "inner")\
    #     .join(portdf,routes.src_airport == portdf.Iata).groupBy(
    #     airdf.airline_id, airdf.name,
    #     routes.src_airport_id,
    #     routes.src_airport).count()
    # takeoff.where(col("count") > 3).distinct().show()
    #
    # # -----sql-----
    #
    routes.createOrReplaceTempView("routedf")
    airdf.createOrReplaceTempView("airlinedf")
    portdf.createOrReplaceTempView("airportdf")
    # spark.sql("select r.airline_id,a.name,r.src_airport_id,r.")

    filter = routes.select(col('src_airport_id'), col('airline_id').cast('int').alias("airline_id"),
                          col('src_airport')) \
        .groupBy('src_airport_id', 'src_airport', 'airline_id').count()\
        .where(r"src_airport_id != '\N' ")\
        .where('count > 3')


    # filter.show()
    # filter.printSchema()

    condition1 = [filter[ 'airline_id' ]  == airdf['airline_id']  ]
    condition2 = [filter[ 'src_airport' ]  == portdf['Iata']  ]

    Result = filter.join(airdf, on=condition1) \
        .select(
        airdf.name.alias('Airline_name'),
        airdf.airline_id,
        filter['src_airport']) \
        .join(portdf, on=condition2) \
        .select(
        'Airline_name',
        airdf.airline_id,
        portdf.Name.alias('Airport_Name')).distinct()
    #
    print(Result.count())
    # Result.show()

    # 4) get airport details which has minimum number of takeoffs and landings
    #
    #
    a = routes.groupBy(col('src_airport'), col('dest_airport')).count()
    b = routes.groupBy(col('src_airport'), col('dest_airport')).count().orderBy(asc('count')).select('count').head(1)[
        0]['count']

    details = portdf.join(a, portdf.Iata == a.src_airport, 'inner').select(
        [portdf.Airport_id, portdf.Name, portdf.City, a.src_airport, a.dest_airport, a['count']]) \
        .filter(a['count'] == b)

    print(details.count())

    # ----sql
    # query = f"""(select ar.name,ar.airport_id,rt.src_airport,rt.dest_airport,count(*) from airportdf ar
    #             join routedf rt on (ar.Iata = rt.src_airport)
    #             group by ar.name,ar.airport_id,rt.src_airport,rt.dest_airport)
    # #             order by count(*) asc"""
    # query = f"""select name,airport_id,src_airport,dest_airport,counts from (select ar.name,ar.airport_id,
    #             rt.src_airport,rt.dest_airport,count(*) as counts,row_number()
    #             over (order by count(*) asc) as r1
    #             from airportdf ar join routedf rt on (ar.Iata = rt.src_airport)
    #             group by ar.name,ar.airport_id,rt.src_airport,rt.dest_airport
    #             order by ar.name)"""
    # spark.sql(query).show()

    # # #### 5) get airport details which is having maximum number of takeoffs and landings
    # #
    a = routes.groupBy(col('src_airport'), col('src_airport_id')).count() \
        .select('src_airport', 'src_airport_id', col('count').alias('takeoffs'))
    b = routes.groupBy(col('dest_airport'), col('dest_airport_id')).count()\
        .select('dest_airport','dest_airport_id',col('count').alias('landings'))
    # a.show()
    # b.show()
    joindf = a.join(b,a['src_airport'] == b['dest_airport'],'full')\
            .select('src_airport','dest_airport',col('takeoffs')+col('landings').alias('total'))\
            .orderBy(desc('(takeoffs + landings AS total)'))
    joindf.show()

    # details = portdf.join(a, portdf.Iata == a.src_airport, 'inner').select(
    #     [portdf.Airport_id, portdf.Name, portdf.City, a.src_airport, a.dest_airport, a['count']]) \
    #     .filter(a['count'] == b)

### ---sql-----------
    query1 = f"""select name,airport_id,src_airport,dest_airport,frequency from (select ar.name,ar.airport_id,
                rt.src_airport,rt.dest_airport,count(*) as frequency,row_number()
                over (order by count(*) desc) as r1
                from airportdf ar join routedf rt on (ar.Iata = rt.src_airport)
                group by ar.name,ar.airport_id,rt.src_airport,rt.dest_airport
                order by ar.name) where r1 = 1"""
    # spark.sql(query1).show()

    ##### 6) Get the airline details which is having direct flights. details like airline id,name,source airport
    # name,destination airport name


    df = routes.join(airdf,routes.airline_id == airdf.airline_id,'inner')\
        .filter(routes['stops'] == 0) \
        .filter(airdf['active'] == 'Y')\
        .select(airdf.airline_id,airdf.name,routes.src_airport,routes.dest_airport)
    df.show()
    # df.printSchema()

    c= routes.join(portdf,routes.src_airport == portdf.Iata)\
        .select(routes.src_airport,portdf['name'].alias('source_airport'))
    d= routes.join(portdf,routes.dest_airport == portdf.Iata)\
        .select(routes.dest_airport,portdf['name'].alias('destination_airport'))

    c.show()
    d.show()



    final = df.join(c,df.src_airport == c.src_airport)\
            .join(d,df.dest_airport == d.dest_airport)\
            .select(df.airline_id,df.name,c.source_airport,d.destination_airport)\
            .orderBy(asc(df['airline_id']))

    # final.show(n=50)
    # airdet = airdf.join(portdf, airdf.iata == portdf.Iata, 'inner') \
    #         .join(a,portdf.Iata == a.src_airport,"inner")\
    #         .select(airdf["airline_id"],airdf["name"],portdf["Name"])
    # airdet.show(truncate = False)


    # print(airdet.count())
