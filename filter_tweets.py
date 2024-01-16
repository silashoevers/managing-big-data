import pandas as pd
from pyspark.sql.functions import col, timestamp_seconds, hour, minute, second, pandas_udf, when
from pyspark.sql.types import IntegerType
#expected input df structure:
# .select('id',
#                     'text',
#                     'lang',
#                     col('user.lang').alias('user_lang'),
#                     col('user.id').alias('user_id'),
#                     col('user.name').alias('username'),
#                     'user.verified',
#                     'timestamp_ms',
#                     'user.time_zone',
#                     'user.utc_offset')

#receives df with tweets and filters for tweets where:
#   The user tweeted in at least 3 time buckets that day (night/morning/afternoon/evening)
#   The tweets were in English or Dutch
#   The tweets contain timezone information
#
# Additionally adds time-of-day information as additional columns


def filter_has_timezone(tweets):
    # Filter based on utc_offset being not null
    return tweets.filter(col('utc_offset').isNotNull())


def add_time_window(sparksession, df_filtered_tweets):
    sparksession.conf.set("spark.sql.session.timeZone", "UTC")

    # Convert into local time timestamps
    df_timestamps = df_filtered_tweets.withColumn('ts', \
            timestamp_seconds(col('timestamp_ms') / 1e3 + col('utc_offset'))) 


    # Extract the hour
    df_timestamps = df_timestamps.withColumns({ 'hour': hour('ts'), 'minute': minute('ts'), 'second': second('ts') })

    # Add time-of-day as boolean flags
    df_timestamp_buckets = df_timestamps.withColumns( {'night': col('hour') < 6,
        'morning': (6 <= col('hour')) & (col('hour') < 12),
        'afternoon': (12 <= col('hour')) & (col('hour') < 18),
        'evening': 18 <= col('hour')})
    return df_timestamp_buckets


def filter_languages(tweets):
    supported_langs = ['nl', 'en']
    tweets = tweets.filter(col('lang').rlike('^' + '|'.join(supported_langs) + '$'))
    return tweets


@pandas_udf(IntegerType())
def filter_enough_time_windows(night: pd.Series, morning: pd.Series, afternoon: pd.Series, evening: pd.Series) -> bool:
    return [night.any(), morning.any(), afternoon.any(), evening.any()].count(True)



def filter_user_tweeting_enough(tweets, min_num_windows = 3):
    user_timewindow_counts = tweets.groupBy('user_id').agg(filter_enough_time_windows(tweets.night, tweets.morning, tweets.afternoon, tweets.evening).alias('time_window_count'))

    user_timewindow_counts = user_timewindow_counts.filter(col('time_window_count') >= min_num_windows)
    return tweets.join(user_timewindow_counts, on='user_id')


def simplify_time_buckets(tweets):
    """
    Turn time buckets from the four boolean values into a single string value,
    which contains one of 'night', 'morning', 'afternoon' or 'evening'
    """
    
    tweets = tweets.withColumn('time_bucket', \
        when(col('night'), 'night').otherwise( \
	    when(col('morning'), 'morning').otherwise( \
  	        when(col('afternoon'), 'afternoon').otherwise('evening'))))
    tweets = tweets.drop('night', 'morning', 'afternoon', 'evening')
    return tweets


def main(sparksession, tweets):
    # Filter for only the langs that we support
    tweets = filter_languages(tweets)

    # Add time window information
    tweets = filter_has_timezone(tweets)
    tweets = add_time_window(sparksession, tweets)

    # Filter for only users that tweeted multiple times during the day (>3)
    # Note: does not work for processing different days
    tweets = filter_user_tweeting_enough(tweets)
    tweets = simplify_time_buckets(tweets)
    return tweets

#expected output df structure:
# .select('id',
#        'text',
#        'lang',
#        col('user.lang').alias('user_lang'),
#        col('user.id').alias('user_id'),
#        col('user.name').alias('username'),
#        'user.verified',
#        'timestamp_ms',
#        'user.time_zone',
#        'user.utc_offset'
#        'user_count',
#        'ts',
#        'hour',
#        'minute',
#        'second',
#	 'time_bucket')
