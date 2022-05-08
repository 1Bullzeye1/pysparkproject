from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").config("spark.driver.bindAddress","localhost")\
            .config("spark.ui.port","4050").getOrCreate()
    # print(spark)

