import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import from_unixtime, year, month, dayofmonth,dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.window import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Decorator function to create a spakr session. Returns the session variable.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    # Set timezone for time-based calculations
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to proccess JSON song data from Udacity S3 bucket to nested parquet files in separate buckets.
    Args: Spark session to run code in, input path to raw data, output path for where to save proccessed data.
    Returns: None
    """
    #Define schema structure for song data
    songs_schema = StructType([ \
        StructField("num_songs",IntegerType(),True),\
        StructField("artist_id",StringType(),False),\
        StructField("artist_latitude",FloatType(),True),\
        StructField("artist_longitude",FloatType(),True),\
        StructField("artist_location",StringType(),True),\
        StructField("artist_name",StringType(),True),\
        StructField("song_id",StringType(),False),\
        StructField("title",StringType(),True),\
        StructField("duration",FloatType(),True),\
        StructField("year",IntegerType(),True)\
        ])
    # get filepath to song data file
    song_data = input_data + "song_data/A/*/*/*.json"
    
    # read song data file
    df = spark.read.schema(songs_schema)\
        .option("recursiveFileLookup", "true")\
        .json(song_data, mode = "DROPMALFORMED")

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table\
    .dropDuplicates()\
    .write\
    .partitionBy("year","artist_id")\
    .mode("overwrite")\
    .parquet(output_data + "Songs/songs.parquet")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
                    .withColumnRenamed('artist_name', 'name')\
                    .withColumnRenamed('artist_location', 'location')\
                    .withColumnRenamed('artist_latitude', 'latitude')\
                    .withColumnRenamed('artist_longitude', 'longitude')\
                    .dropDuplicates()
    
    
    # write artists table to parquet files
    artists_table\
    .dropDuplicates()\
    .write\
    .mode("overwrite")\
    .parquet(output_data + "Artists/artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Function to proccess JSON log data from Udacity S3 bucket to nested parquet files in separate buckets.
    Addittionally, this function reads in the same somg data to join it to use log data to create the Songplays table 
    which would act as the Fact table in a database if we were using RDBMS solution instead of Spark.
    Args: Spark session to run code in, input path to raw data, output path for where to save proccessed data.
    Returns: None
    """
    
    #Define schema structure for log data
    logs_schema = StructType([ \
    StructField("artist",StringType(),True),\
    StructField("auth",StringType(),True),\
    StructField("firstName",StringType(),True),\
    StructField("gender",StringType(),True),\
    StructField("itemInSession",IntegerType(),True),\
    StructField("lastName",StringType(),True),\
    StructField("length",FloatType(),True),\
    StructField("level",StringType(),True),\
    StructField("location",StringType(),True),\
    StructField("method",StringType(),True),\
    StructField("page",StringType(),False),\
    StructField("registration",FloatType(),True),\
    StructField("sessionId",IntegerType(),True),\
    StructField("song",StringType(),True),\
    StructField("status",IntegerType(),True),\
    StructField("ts",LongType(),False),\
    StructField("userAgent",StringType(),True),\
    StructField("userId",StringType(),False)\
    ])
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.schema(logs_schema)\
        .json(log_data, mode = "DROPMALFORMED", lineSep = "\n")
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')\
                    .withColumnRenamed('userId', 'user_id')\
                    .withColumnRenamed('firstName', 'first_name')\
                    .withColumnRenamed('lastName', 'last_name')
    
    # write users table to parquet files
    users_table\
    .dropDuplicates()\
    .write\
    .mode("overwrite")\
    .parquet(output_data + "Users/users.parquet")

    # create timestamp column from original timestamp column in the USA/LA Timezone
    df = df.withColumn('start_time', from_unixtime(df.ts))

    # extract columns to create time table
    time_table = df.select('start_time')\
        .withColumn('hour', hour(df.start_time))\
        .withColumn('day', dayofmonth(df.start_time))\
        .withColumn('week', weekofyear(df.start_time))\
        .withColumn('month', month(df.start_time))\
        .withColumn('year', year(df.start_time))\
        .withColumn('weekday', dayofweek(df.start_time)%7 < 2)
    
    # write time table to parquet files partitioned by year and month
    time_table\
    .dropDuplicates()\
    .write\
    .partitionBy("year","month")\
    .mode("overwrite")\
    .parquet(output_data + "Time/time.parquet")

    # read in song data to use for songplays table
    songs_schema = StructType([ \
        StructField("num_songs",IntegerType(),True),\
        StructField("artist_id",StringType(),False),\
        StructField("artist_latitude",FloatType(),True),\
        StructField("artist_longitude",FloatType(),True),\
        StructField("artist_location",StringType(),True),\
        StructField("artist_name",StringType(),True),\
        StructField("song_id",StringType(),False),\
        StructField("title",StringType(),True),\
        StructField("duration",FloatType(),True),\
        StructField("year",IntegerType(),True)\
        ])
    # get filepath to song data file
    song_data = song_data = input_data + "song_data/A/*/*/*.json"
    
    # read song data file
    song_df = spark.read.schema(songs_schema)\
        .option("recursiveFileLookup", "true")\
        .json(song_data, mode = "DROPMALFORMED")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                        select 
                        start_time,
                        year(start_time) as year,
                        month(start_time) as month, 
                        user_id, 
                        level, 
                        song_id, 
                        artist_id, 
                        session_id, 
                        location, 
                        user_agent
                        from df
                        join song_df
                        on (df.artist = song_df.artist_name and df.song = song_df.title)
                        order by start_time desc
                        """)
    
    #Add a songplay_id column which counts the number of plays of a song as ordered by the time the song was played
    songplays_table = songplays_table.withColumn('songplay_id',row_number().over(Window.partitionBy("song_id").orderBy(col("start_time"))))
    
    songplays_table =  songplays_table.select('songplay_id','start_time', 'year','month','user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table\
    .dropDuplicates()\
    .write\
    .partitionBy("year","month")\
    .mode("overwrite")\
    .parquet(output_data + "Songplays/songplays.parquet")


def main():
    """
    Function to run ELT proccess. This function creates a Spark session, then calls the data proccessing functions.
    Finally, this function will close the spark session.
    Args: None
    Returns: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-678848124427-us-east-1/TempData/DataLakeETL/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == "__main__":
    main()
