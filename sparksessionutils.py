from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").config("spark.driver.bindAddress","localhost")\
            .config("spark.ui.port","4050").getOrCreate()
    # print(spark)

    airlineschema = StructType([StructField("airline_id", IntegerType()),
                                StructField("name", StringType()),
                                StructField("alias", StringType()),
                                StructField("iata", StringType()),
                                StructField("icao", StringType()),
                                StructField("callsign", StringType()),
                                StructField("country", StringType()),
                                StructField("active", StringType())])
    airdf = spark.read.csv(r"D:\pysparkproject\input\airline.csv", schema=airlineschema, header=True)
    airdf.printSchema()
    airdf.show()
