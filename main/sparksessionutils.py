
class sparksessionutils:

    def readCsv(self,spark,path,schema=None, inferschema=True,header=True,sep=","):

        if (inferschema is False) and (schema == None):
            raise Exception("please provide Schema as True or else provide schema for given i/p file")

        if schema == None:

            readdf = spark.read.csv(path = path,inferSchema=inferschema,header=header,sep=sep)
        else:
            readdf = spark.read.csv(path= path,schema=schema,header=header,sep=sep)
        return readdf

