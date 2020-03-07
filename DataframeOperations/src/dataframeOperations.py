from pyspark.sql.functions import expr, col, sum, to_date, desc, concat, lit, round, avg
from pyspark.sql.functions import explode, array, unix_timestamp, to_timestamp, date_format
from util.df_logger import get_logger
from configparser import ConfigParser
import os

logger = get_logger(__name__)

# Setting up application config file
config = ConfigParser()
CFG_DIR = os.environ.get('CFG_DIR')
if CFG_DIR:
    CFG_FILE = CFG_DIR + "\\config.properties"
else:
    CFG_FILE = "E:\\Spark\\github\\Pyspark\\DataframeOperations\\conf\\config.properties"
try:
    config.read(CFG_FILE)
    # Read DB values from config file
    if 'DB' in config:
        db_cfg = config['DB']
        db_url = db_cfg['DB_URL']
    else:
        db_url = "jdbc:sqlite:E:\\Spark\\github\\Pyspark\\DataframeOperations\\resources\\df.db"
    # Read fare details from config files
    if 'APP' in config:
        app_cfg = config['APP']
        distance_fare = float(app_cfg['DISTANCE_FARE'])
        time_fare = float(app_cfg['TIME_FARE'])
    else:
        distance_fare = 0.2
        time_fare = 0.3
except Exception as error:
    logger.exception(f"Failed to get values from the CFG file {CFG_FILE} {error}")
    raise Exception
else:
    logger.info(f"Completed reading values from the CFG file {CFG_FILE}")


class FlightOperations:
    '''This class contains reads csv files and creates DF for some data manipulation
    It requires 3 parameter to instatiate an object
    Args - Active spark session, location of the json files and the schema of the dataframe to create'''

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
        logger.info(f"Getting destinations for {origin}")
        dest_df = self.df.where(expr('ORIGIN_COUNTRY_NAME') == origin).select(
            'DEST_COUNTRY_NAME').distinct()
        return dest_df

    def get_origins(self, destination):
        '''Returns the list of origin countries for the country given'''
        logger.info(f"Getting origins for {destination}")
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


class BikeOperations:
    '''This class contains reads csv files and creates DF for some data manipulation
    It requires 2 parameter to instatiate an object
    Args - Active spark session and location of the csv files'''

    def __init__(self, spark, filePath):
        try:
            self.spark = spark
            self.bike_df = self.spark.read.option("header", "true").format("csv").load(filePath)
        except Exception as error:
            logger.exception(f"Failed to load CSV files {error}")
        else:
            logger.info("CSV files are loaded successfully")

    def get_busiest_start_terminal(self):
        '''Returns the most busiest start terminal'''
        busy_st_df = self.bike_df.groupBy("`Start Station`").count().sort(desc("count")).limit(1)
        return busy_st_df.select(concat(col("`Start Station`"), lit(" contains most start trips - "), col("count")))

    def get_busiest_end_terminal(self):
        '''Returns the most busiest end terminal'''
        busy_end_df = self.bike_df.groupBy("`End Station`").count().sort(desc("count")).limit(1)
        return busy_end_df.select(concat(col("`End Station`"), lit(" contains most start trips - "), col("count")))

    def get_fare_by_distance(self, start, end):
        '''Returns the estimated fare for the given start and end station
        Args: Start and End station names - both are mandatory'''
        station_list = self.bike_df.select(explode(array("`Start Terminal`", "`End Terminal`")).alias("station_id"))\
            .where(col("`Start Station`").isin(start, end) & col("`End Station`").isin(start, end)).dropDuplicates()\
            .select(col("station_id").cast("integer")).sort(desc("station_id")).collect()
        if len(station_list) == 2:
            return station_list[0].station_id - station_list[1].station_id * distance_fare
        return 0

    def get_fare_by_time(self, trip_id):
        '''Returns the calculated fare for the given trip id
        Args: Trip ID - mandatory
        Returns: Caluclated fare when the trip is exist in the data, otherwise it returns 0'''
        minutes = self.bike_df.where(col("`Trip ID`") == trip_id)\
            .select(((unix_timestamp(to_timestamp('`End Date`', "MM/dd/yyyy HH:mm"))
                      - unix_timestamp(to_timestamp('`Start Date`', "MM/dd/yyyy HH:mm")))/60).alias("tot_minutes")).collect()
        if len(minutes) > 0:
            time_intervals = minutes[0].tot_minutes // 15
            bal_minutes = minutes[0].tot_minutes % 15
            if bal_minutes >= 1:
                time_intervals += 1
            return time_intervals * time_fare
        return 0

    def predict_no_trips(self, weekday):
        '''Retruns the average number of trips of the week day from the past trip details
        Args: Day of the week - mandatory
        Returns: Predicted number of trips for the given day'''
        predict_trip_count = self.bike_df.select(to_date(col('`Start Date`'), 'MM/dd/yyyy').alias("Start_date"),
                                                 date_format(to_date(col('`Start Date`'), 'MM/dd/yyyy'), 'EEEE').alias("week_day"))\
            .where(col("week_day") == weekday.title()).groupBy("Start_date").count().\
            select(round(avg("count"), 0).cast('integer').alias("predicted_trips"))
        return predict_trip_count.collect()[0].predicted_trips

    def get_no_bikes(self):
        '''Return the total number bikes in use'''
        return self.bike_df.select("`Bike #`").distinct().count()

    def get_no_trips(self, start_terminal, end_terminal):
        '''Returns the total number of trips between 2 stations
        Args: Start and End station codes - Mandatory'''
        return self.bike_df.where(expr('`Start Station`') == start_terminal).where(expr('`End Station`') == end_terminal).count()

    def store_station_details(self, tablename):
        '''Updates the table with the latest station details
        Args: name of the station table - mandatory
                Returns: 0-Success'''
        station_ids = self.bike_df.select(explode(array("`Start Terminal`", "`End Terminal`")).alias("station_id"))\
            .dropDuplicates().na.drop()
        join_expression1 = station_ids["station_id"] == self.bike_df["`Start Terminal`"]
        join_expression2 = station_ids["station_id"] == self.bike_df["`End Terminal`"]
        station_details = self.bike_df.join(station_ids, join_expression1)\
            .select(col("`Start Terminal`").cast('integer').alias("id"), "`Start Station`")\
            .union(self.bike_df.join(station_ids, join_expression2)
                   .select(col("`End Terminal`").cast('integer').alias("id"), "`End Station`"))\
            .dropDuplicates(["id"]).orderBy("id").coalesce(1)

        try:
            if db_url and tablename:
                station_details.write.mode("overwrite").format("jdbc").options(
                    url=db_url, dbtable=tablename, batchsize=1000).save()
            else:
                logger.exception("Database details are not configured correctly")
                raise Exception
        except Exception as error:
            logger.exception(f"Failed to update the station details {error}")
            raise Exception
        else:
            logger.info("Station details are updated")
            return 0
