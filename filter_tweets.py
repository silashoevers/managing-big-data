from pyspark.sql.functions import col
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
    return tweets_2

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
#                     'user_count')
