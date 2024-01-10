from pyspark.sql.functions import col, timestamp_seconds, hour, minute, second
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

#receives df with tweets and filters out the users and tweets where:
#   The user tweeted at least 3 times that day
#   Those tweets were at least 4 hours apart each
#   The tweets were in English or Dutch
#   The tweets contain timezone information
#
# Additionally adds time-of-day information as additional columns

def add_time_window(sparksession, df_filtered_tweets):
    sparksession.conf.set("spark.sql.session.timeZone", "UTC")

    # Filter based on timestamp being not null
    df_filtered_tweets = df_filtered_tweets.filter(col('utc_offset').isNotNull())

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


def main(sparksession,all_tweets):
    # Filter for only the langs that we support
    supported_langs = ['nl', 'en']
    # TODO: allow language tags like nl-be, en-us and en-gb
    tweets = all_tweets.filter(col('lang').rlike('^' + '|'.join(supported_langs) + '$'))

    # Filter for only users that tweeted multiple times during the day (>3)
    # TODO expand to work across multiple days (group by user and day)
    user_tweet_counts = tweets.groupBy('user_id').count().filter(col('count') > 3)

    # TODO: Filter out the tweets that are not spread out enough through the day

    # Only take the tweets from users who tweeted enough times
    tweets_2 = tweets.join(user_tweet_counts, on='user_id').withColumnRenamed('count','user_count')

    # Add time window information
    tweets_3 = add_time_window(sparksession, tweets_2)
    return tweets_3

#expected output df structure:
# .select('id',
#                     'text',
#                     'lang',
#                     col('user.lang').alias('user_lang'),
#                     col('user.id').alias('user_id'),
#                     col('user.name').alias('username'),
#                     'user.verified',
#                     'timestamp_ms',
#                     'user.time_zone',
#                     'user.utc_offset'
#                     'user_count',)
#                     'ts'
#                     'hour'
#                     'minute'
#                     'second'
#                     'night'
#                     'morning'
#                     'afternoon'
#                     'evening'
