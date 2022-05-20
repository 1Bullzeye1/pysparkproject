from airlineproject import *
from airlinedf import *
from airportdf import *


## 2) find the country name which is having both airlines and airport

    # ---1st method

df1 = airlinedf()
df2 = airportdf()

spark = df1.sparks()

airline = df1.airlines()
airport = df2.airports()

output = airline.join(airport, airline.country == airport.Country,"inner") \
            .select(airline.country.alias("Country"), airport.Name.alias("Airport_name"), airline.name.alias("Airline_name"))

# output.show()
# output.printSchema()

# ---2nd way of representation

airpdf = output.groupBy("Country").agg(concat_ws(", ", collect_list("Airport_name")) \
        .alias("Airport_Name"))
# airpdf.show()

airldf = output.groupBy("Country").agg(concat_ws(", ", collect_list("Airline_name")) \
        .alias("Airline_Name"))
# airldf.show()

joindf = airpdf.join(airldf, airpdf.Country == airldf.Country, "inner") \
        .select(airpdf["Country"], "Airport_Name", "Airline_Name")
print(joindf.count())

# joidf.write.csv(r"output/countairport",header=True)
# joidf.write.json(r"output/jsondf")
# csvdf = spark.read.csv(r"output/countairport/", header=True)
# csvdf.show()


# 3rd method-----------SQL--------
airport.createOrReplaceTempView("airport")
airline.createOrReplaceTempView("airline")
# spark.sql("select * from airport").show()
# spark.sql("select * from airline").show()
print("----------sql---------------- ")
query = """select distinct ar.Country as Country from airline al
            join airport ar on (al.country = ar.Country)
            """
sqldf = spark.sql(query)

print(sqldf.count())

# sqldf.printSchema()
