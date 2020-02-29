
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from flightOperations import FlightOperations
from util.flight_logger import get_logger

if __name__ == "__main__":
    try:
        logger = get_logger(__name__)
        
        spark = SparkSession.builder.appName("FlightDetails").getOrCreate()
        if spark:
            logger.info("Spark session has created")
            manualSchema = StructType([StructField("DEST_COUNTRY_NAME", StringType(), True),
                               StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
                               StructField("count", LongType(), False, metadata={"hello": "world"})])
            flightoperations = FlightOperations(spark,"E:\\Spark\\Working\\data\\flight-data\\json", manualSchema)

            '''Convert Spark DF into python list'''
            origin_country = 'india'.title()
            destination_df = flightoperations.get_destinations(origin_country)
            if len(destination_df.head(1)) > 0:
                #destination_df.show(destination_df.count())
                dest_list = [ dest.DEST_COUNTRY_NAME for dest in destination_df.collect()]
                print(dest_list)
                logger.info(f"Extracted the filghts available from {origin_country}")
            else:
                logger.warn(f"No flight is available from {origin_country}")

            '''Save Spark DF into a JSON file'''    
            origin_df = flightoperations.get_origins('india'.title())
            if len(origin_df.head(1)) > 0:
                origin_df.repartition(1).write.format('json').save("E:\\Spark\\project1\\origin.json",mode="Overwrite")
            else:
                logger.warn("No flight has found")

            '''Convert Spark DF into python int'''
            print(flightoperations.get_flight_origin_count('India'))

            '''To get aggregated sum of the result'''
            result = flightoperations.get_no_of_flights('united states'.title(),'australia'.title())
            if result:
                print(result)
            else:
                logger.warn('No flight service')
   # except flightOperations.FileLoadingException:
    #    logger.exception(flightOperations.FileLoadingException.__str__)
    except Exception as error:
        logger.exception(f"Something went wrong here {error}")
    else:
        logger.info("Flight operations have completed")
    finally:
        spark.stop()


