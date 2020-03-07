
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from dataframeOperations import FlightOperations
from util.df_logger import get_logger

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
                dest_list = [ dest.DEST_COUNTRY_NAME for dest in destination_df.collect()]
                logger.info(f"Extracted the filghts available from {origin_country}")
                logger.info(dest_list)
            else:
                logger.warn("No flight is available from " + origin_country)

            '''Save Spark DF into a JSON file'''    
            logger.info("Checking flights to India...")
            origin_df = flightoperations.get_origins('india'.title())
            if len(origin_df.head(1)) > 0:
                logger.info("Writing the output file to E:\\Spark\\project1\\origin.json")
                origin_df.repartition(1).write.format('json').save("E:\\Spark\\project1\\origin.json",mode="Overwrite")
            else:
                logger.warn("No flight has found")

            '''Convert Spark DF into python int'''
            logger.info("Checking total number of unique countries which have flights to India...")
            logger.info(flightoperations.get_flight_origin_count('India'))

            '''To get aggregated sum of the result'''
            logger.info(f"Checking total number of flights between {'united states'.title()} and {'australia'.title()}")
            result = flightoperations.get_no_of_flights('united states'.title(),'australia'.title())
            if result:
                logger.info(result)
            else:
                logger.warn("No flight service")
    except Exception as error:
        logger.exception(f"Something went wrong here {error}")
    else:
        logger.info("Flight operations have completed")
    finally:
        spark.stop()


