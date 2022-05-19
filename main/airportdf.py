from airlinedf import *




class airportdf(sparkinit):

    def airports(self):
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

        portdf = rdf.readCsv(self.sparks(),path="../input/airport.csv", schema=airportschema, header=True)
        return portdf