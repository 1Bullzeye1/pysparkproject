
class sparksessionutils:

    def readCsv(self,spark,path,schema=None, inferSchema=True,header=True,sep=","):

        if (inferSchema is False) and (schema == None):
            raise Exception("please provide Schema as True or else provide schema for given i/p file")

        if schema == None:
            readdf = spark.read.csv(path = path,inferSchema=inferSchema,header=header,sep=sep)
        else:
            readdf = spark.read.csv(path= path,schema=schema,header=header,sep=sep)
        return readdf




# rdu = sparksessionutils()

# df = spark.read.csv(r"C:\Users\Administrator\Downloads\jobs.csv", inferSchema=True, header=True)
# # df.show()
# # df.printSchema()
#
# rdd1 = df.rdd
# # print(rdd1.collect())
#
#
# a = rdd1.map(list)
# res = a.filter(lambda x : (x[2]) > 10000)
# print(res.collect())