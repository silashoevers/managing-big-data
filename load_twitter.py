from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()

PATH = '/data/doina/Twitter-Archive.org/2017-01/01/*/*.json.bz2'


def get_relevant_tweets_for_day(sparksession, path):
    """
    """
    all_tweets = spark.read.json(path) \
            .select('id',
                    'text',
                    'lang',
                    col('user.lang').alias('user_lang'),
                    col('user.id').alias('user_id'),
                    col('user.name').alias('username'),
                    'user.verified',
                    'timestamp_ms',
                    'user.time_zone',
                    'user.utc_offset')

    # Filter for only the langs that we support
    supported_langs = ['nl', 'en']
    tweets = all_tweets.filter(col('lang').rlike('^' + '|'.join(supported_langs) + '$'))

    # Filter for only users that tweeted multiple times during the day (>5)
    user_tweet_counts = tweets.groupBy('user_id').count().filter(col('count') > 5)

    # TODO: Filter out the tweets that are not spread out enough through the day

    # Only take the tweets from users who tweeted enough times
    tweets_2 = tweets.join(user_tweet_counts, on='user_id')
    return tweets_2, user_tweets_count


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    get_relevant_tweets_for_day(spark, PATH).show()
