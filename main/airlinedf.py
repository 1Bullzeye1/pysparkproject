from airlineproject import *

class airlinedf(sparkinit):

    def airlines(self):

        # df = dataframe.rdf

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

        airdf = rdf.readCsv(spark=self.sparks(), path="../input/airline.csv", schema=airlineschema, header=True)
        return airdf
# a = airlinedf()
# a.airlines()