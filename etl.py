import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType, FloatType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Create and return SparkSession object
    
    Returns:
        TYPE: Spark Session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process data from the song JSON files into songs and 
       artist tables in the star schema.
    
    Args:
        spark (SparkSession): SparkSessionObject
        input_data (str): location of JSON files.
        output_data (str): location to write parquet files.
    """
    # get filepath to song data file
    
    print("Processing song data...")
    
    song_data = input_data + "song-data/*/*/*/*.json"
    
    #Define a schema for the data
    schema = StructType([
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_longitude', DoubleType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('year', DoubleType(), True),
    ])
    
    print("Reading files... {}".format(song_data))
    # read song data file
    df = spark.read.json(song_data, schema=schema)

    # extract columns to create songs table
    print("Selecting song table")
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')
    
    print("Writing songs table")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs.parquet", 
                                                               mode="overwrite")
    
    print("Selecting artist table")
    # extract columns to create artists table
    artist_table = df.select('artist_id', 
                             col('artist_name').alias('name'), 
                             col('artist_location').alias('location'),
                             col('artist_latitude').alias('latitude'), 
                             col('artist_longitude').alias('longitude')).distinct()
    
    print("Writing artist table")
    # write artists table to parquet files
    artist_table.write.parquet(output_data + "artist.parquet", 
                              mode="overwrite")

    print("Finished processing song data...")
    
def process_log_data(spark, input_data, output_data):
    """Process data from the log and song JSON files into users, time and 
       song_plays tables in the star schema.
    
    Args:
        spark (SparkSession): SparkSessionObject
        input_data (str): location of JSON files.
        output_data (str): location to write parquet files.
    """

     # get filepath to log and song data file
    log_data = input_data + "log-data/*/*/*.json"
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # schema for log files
    schema = StructType([
        StructField('artist', StringType(), True),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', LongType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', FloatType(), True),
        StructField('sessionId', IntegerType(), True),
        StructField('song', StringType(), True),
        StructField('status', IntegerType(), True),
        StructField('ts', FloatType(), True),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), True),
    ])
    
    
    # read log data file
    print("Reading files... {}".format(log_data))
    df = spark.read.json(log_data, schema=schema)
    
    # filter by actions for song plays
    print("Filtering log data")
    df_filtered = df.filter(df.page =='NextSong')

    # extract columns for users table 
    print("Extracting users table columns")
    users_table = df_filtered.select(col('userId').alias('user_id'),
                                     col('firstName').alias('first_name'),
                                     col('lastName').alias('last_name'),
                                     'gender',
                                     'level').distinct() 
    
    # write users table to parquet files
    print("Writing users table")
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite") 

    # create timestamp column from original timestamp column
    #https://stackoverflow.com/questions/49971903/converting-epoch-to-datetime-in-pyspark-data-frame-using-udf
    print("Converting to timestamp")
    ts_format = "yyyy-MM-dd HH:MM:ss z"
    df = df_filtered.withColumn('ts', to_timestamp(date_format((df.ts / 1000).cast(dataType=TimestampType()), ts_format), ts_format))   

    # extract columns to create time table
    print("Selecting for time table...")
    time_table = df.select(col('ts').alias('start_time'),
                           hour(col('ts')).alias('hour'),
                           dayofmonth(col('ts')).alias('day'),
                           weekofyear(col('ts')).alias('week'),
                           month(col('ts')).alias('month'),
                           year(col('ts')).alias('year')).distinct() 
    
    time_table = time_table.withColumn('weekday', date_format('start_time','u'))
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table...")
    time_table.write.partitionBy('year','month').parquet(output_data + "time.parquet", mode="overwrite")
    
    #schema for song files
    song_schema = StructType([
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_longitude', DoubleType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('year', DoubleType(), True),
    ])

    # read in song data to use for songplays table
    print("Reading file... {}".format(song_data))
    song_df = spark.read.json(song_data, schema=song_schema)

    #join tables
    print("Joining songs and log data")
    song_plays = song_df.join(df, (df.artist == song_df.artist_name) 
                                  & (df.song == song_df.title)
                                  & (df.length == song_df.duration))
   
    #Add an increasing ID
    print("Adding ID to songplays")
    song_plays = song_plays.withColumn('songplay_id', monotonically_increasing_id())
    
    #Select appropriate columns
    print("Selecting columns for songplay")
    song_plays = song_plays.select(col('songplay_id'),
                                   col('ts').alias('start_time'),
                                   col('userId').alias('user_id'),
                                   col('level'),
                                   col('song_id'),
                                   col('artist_id'),
                                   col('sessionId').alias('session_id'),
                                   col('location'),
                                   col('userAgent').alias('user_agent'),
                                   month(col('ts')).alias('month'),
                                   year(col('ts')).alias('year'))
    
    print("Writing songplays to file...")
    song_plays.write.partitionBy('year','month').parquet(output_data + 'song_plays.parquet', mode="overwrite")

    
def main():
    """Main pipeline begins here, take data from input and transform to output location."""
    
    spark = create_spark_session()
    input_data =  "s3a://udacity-dend/"
    output_data = "s3a://dend-test-us-west/"
    output_data = "data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
