from airlinedf import *



class planesdf(sparkinit):

    def planes(self):
        plane = rdf.readCsv(spark=self.sparks(), path="../input/plane.csv", inferSchema=True, sep='', header=True)
        return plane



