from pyspark.sql.functions import expr, col, sum
from util.flight_logger import get_logger

logger = get_logger(__name__)

class FlightOperations:
    def __init__(self, spark, filePath, manualSchema):
        try:
            self.spark = spark
            self.df = self.spark.read.schema(manualSchema).format('json').load(filePath)
        except Exception as error:
            logger.exception(f"Failed to load json files {error}")
            raise
        else:
            logger.info("JSON files are loaded successfully")

    def get_destinations(self, origin):
        '''Returns the list of destination countries from the country given'''
        '''if origin:
            dest_df = df.where(expr('DEST_COUNTRY_NAME') == origin).select('ORIGIN_COUNTRY_NAME').distinct()
        else:
            print("Length is zero")
            dest_schema = StructType([StructField("DESTNIATIONS",StringType(),True)])
           # no_data = [Row("No flight")]
            dest_df = self.spark.createDataFrame([],dest_schema)'''
        logger.info(f'Getting destination for {origin}')
        dest_df = self.df.where(expr('ORIGIN_COUNTRY_NAME') == origin).select(
            'DEST_COUNTRY_NAME').distinct()
        return dest_df

    def get_origins(self, destination):
        '''Returns the list of origin countries for the country given'''
        return self.df.where(expr('DEST_COUNTRY_NAME') == destination).select('ORIGIN_COUNTRY_NAME').distinct()

    def get_flight_dest_count(self, origin):
        '''Returns the total number of destinations available from the origin passed'''
        return self.get_destinations(origin).count()

    def get_flight_origin_count(self, dest):
        '''Returns the total number of origins available from the destination passed'''
        return self.get_origins(dest).count()

    def get_no_of_flights(self, origin, destination):
        '''Returns the total number of flights between the countries passed'''
        no_of_flights = self.df.where(expr('DEST_COUNTRY_NAME') == destination).where(expr('ORIGIN_COUNTRY_NAME') == origin)\
            .agg(sum(col('count')).alias('Count')).collect()[0]
        # print(type(no_of_flights))
        return no_of_flights.Count
    