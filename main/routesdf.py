from airlinedf import *


class routesdf(sparkinit):

    def routes(self):
        sp =self.sparks()
        routes = sp.read.parquet(r"../input/routes.snappy.parquet")
        return routes
