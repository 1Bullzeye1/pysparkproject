from pyspark.sql import *
from sparksessionutils import sparksessionutils
from pyspark.sql.functions import *
from pyspark.sql.types import *


rdf = sparksessionutils()
##----------Mayuresh Sonawane-------------##

class sparkinit:

    def sparks(self):

        sparks = SparkSession.builder\
                .master("local[*]")\
                .config("spark.driver.memory","8g")\
                .appName("airlinemgmt") \
                .getOrCreate()
        return sparks