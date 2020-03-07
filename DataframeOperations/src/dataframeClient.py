
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from dataframeOperations import FlightOperations
from dataframeOperations import BikeOperations
from util.df_logger import get_logger

if __name__ == "__main__":
    try:
        logger = get_logger(__name__)

        spark = SparkSession.builder.appName("DataframeOperations").getOrCreate()
        if spark:
            logger.info("Spark session has created")
            manualSchema = StructType([StructField("DEST_COUNTRY_NAME", StringType(), True),
                                       StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
                                       StructField("count", LongType(), False, metadata={"hello": "world"})])
            flightoperations = FlightOperations(
                spark, "E:\\Spark\\Working\\data\\flight-data\\json", manualSchema)
            bike_operations = BikeOperations(spark, "E:\\Spark\\Working\\data\\bike-data")

            # Convert Spark DF into python list
            origin_country = 'india'.title()
            destination_df = flightoperations.get_destinations(origin_country)
            if len(destination_df.head(1)) > 0:
                dest_list = [dest.DEST_COUNTRY_NAME for dest in destination_df.collect()]
                logger.info(f"Extracted the filghts available from {origin_country}")
                logger.info(dest_list)
            else:
                logger.warn("No flight is available from " + origin_country)

            # Save Spark DF into a JSON file
            logger.info("Checking flights to India...")
            origin_df = flightoperations.get_origins('india'.title())
            if len(origin_df.head(1)) > 0:
                logger.info("Writing the output file to E:\\Spark\\project1\\origin.json")
                origin_df.repartition(1).write.format('json').save(
                    "E:\\Spark\\project1\\origin.json", mode="Overwrite")
            else:
                logger.warn("No flight has found")

            # Convert Spark DF into python int
            logger.info("Checking total number of countries which have flights to India...")
            logger.info(flightoperations.get_flight_origin_count('India'))

            # To get aggregated sum of the result
            logger.info(
                f"Checking total number of flights between {'united states'.title()} and {'australia'.title()}")
            result = flightoperations.get_no_of_flights(
                'united states'.title(), 'australia'.title())
            if result:
                logger.info(result)
            else:
                logger.warn("No flight service")

            # Fetch the busiest start terminal
            logger.info("Checking for the most busiest journey begin place")
            busy_st_df = bike_operations.get_busiest_start_terminal()
            logger.info(busy_st_df.take(1))

            # Fetch the busiest end terminal
            logger.info("Checking for the most busiest journey end place")
            busy_end_df = bike_operations.get_busiest_end_terminal()
            logger.info(busy_end_df.take(1))

            # Get number of trips between start and end locations
            no_trips = bike_operations.get_no_trips("Harry Bridges Plaza (Ferry Building)",
                                                    "San Francisco Caltrain (Townsend at 4th)")
            logger.info(
                f"Total number of trips between Harry Bridges Plaza (Ferry Building) and San Francisco Caltrain (Townsend at 4th) is {no_trips}")

            # Get total number of available bikes
            no_bikes = bike_operations.get_no_bikes()
            logger.info(f"Total of number of bikes available is {no_bikes}")

            # Predict the number of trips in a day
            predicted_count = bike_operations.predict_no_trips("FRIDAY")
            logger.info(f"Predicted trips count for Friday is {predicted_count}")

            # Calculate fare for a trips
            fare = bike_operations.get_fare_by_time(913459)
            logger.info(f"Calculated fare for the trip id:913459 is {fare}")

            # Estimate fare between places
            fare = bike_operations.get_fare_by_distance(
                "San Antonio Shopping Center", "Paseo de San Antonio")
            logger.info(
                f"Estimated fare between San Antonio Shopping Center and Paseo de San Antonio is {fare}")

            # Update the station details in a table
            bike_operations.store_station_details("station_details")

    except Exception as error:
        logger.exception(f"Something went wrong here {error}")
    else:
        logger.info("Dataframe operations have completed")
    finally:
        spark.stop()
