import configparser
from datetime import datetime, date
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']= config['AWS']['AWS_SECRET_ACCESS_KEY']



# Starts a new Spark Session
def create_spark_session():
    print('\n\Spark Session Starting...\n\n')
    '''
    Function: 
        1) initialize spark session builder
        2) configure session parameters
        3) create or get session if exists
    Parameters:
        None
    
    Returns:
        spark session curser
    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark




def https_request():
    






# Process song data files
def process_song_data(spark, input_data, output_data):
    print('\n\nProcessing Song Data...\n\n')
    '''
    Function: 
        1) loads input JSON files from S3: song_data 
        2) extracts data columns
        3) write songs and artists parquet files to S3
    Parameters:
        spark: the cursor
        input_path: path to the S3 bucket with song data
        output_path: path to S3 bucket where parquet files are stored
    
    Returns:
        None
    '''

    
    ## STEP 1 - LOAD INPUT DATA

    song_data = input_data+'/song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)
    print('Completed: input file song_data loaded from S3')



    ## STEP 2 - EXTRACT DIMENSIONAL COLUMNS

    # songs - songs in music database
        # song_id, title, artist_id, year, duration
    
    # extract columns to create songs table
    songs_table = df.select(
        'song_id', 'title', 'artist_id', 'year', 'duration'
        ).dropDuplicates()
    print('Completed: songs_table columns extracted')

    # artists - artists in music database
        # artist_id, name, location, lattitude, longitude
    
    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
        ).dropDuplicates()
    print('Completed: artists_table columns extracted')



    ## STEP 3 - WRITE PARQUET FILES

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'/songs_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])
    print('Completed: songs_table written to S3')

    # write artists table to parquet files
    artists_table.write.parquet(output_data+'/artists_table',
                              mode='overwrite')
    print('Completed: artists_table written to S3')


    print('\n\nCompleted: song data processed!\n\n')