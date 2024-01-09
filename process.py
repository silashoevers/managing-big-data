from pyspark.sql.functions import timestamp_seconds, col, hour, minute, second
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
#                     'user.utc_offset'
#                     'user_count')

#receives df with filtered tweets and:
#   1) Checks number of spelling mistakes in each tweet
#   2) Calculates spelling mistakes as % of words in the tweet
#   3) Add tweet time window
#       Time windows: 
#           Morning     (06:00-12:00)
#           Afternoon   (12:00-18:00)
#           Evening     (18:00-00:00)
#           Night       (00:00-06:00)


def add_time_window(sparksession, df_filtered_tweets):
    # TODO move this to filter_tweets.py

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

    df_timestamp_buckets.sort('timestamp_ms', ascending=True).show()
    return df_timestamp_buckets

def process_spelling_mistakes(sparksession, df_time_added):
    # Todo
    return df_time_added

def main(sparksession, df_filtered_tweets):
    df_time_added = add_time_window(sparksession, df_filtered_tweets)
    df_spelling_processed = process_spelling_mistakes(sparksession, df_time_added)
    return df_spelling_processed

#expected output df structure:
# ('user_id', 
#   'username', 
#   'tweet_id',
#   'spelling_mistakes',
#   'percentage_wrong',
#   'time_bucket',
#   'lang')
