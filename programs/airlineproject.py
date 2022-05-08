from programs.sparksessionutils import spark


airlineschema = StructType([StructField("airline_id", IntegerType()),
                            StructField("name", StringType()),
                            StructField("alias", StringType()),
                            StructField("iata", StringType()),
                            StructField("icao", StringType()),
                            StructField("callsign", StringType()),
                            StructField("country", StringType()),
                            StructField("active", StringType())])
airdf = spark.read.csv(r"D:\airline data\airline.csv",schema=airlineschema,header=True)
airdf.printSchema()
airdf.show()



