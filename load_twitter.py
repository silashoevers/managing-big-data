from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# spark = SparkSession.builder.getOrCreate()

# PATH = '/data/doina/Twitter-Archive.org/2017-01/01/*/*.json.bz2'


def get_relevant_tweets_for_day(sparksession, path):
    """
    """
    all_tweets = sparksession.read.json(path) \
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

    return all_tweets


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    get_relevant_tweets_for_day(spark, PATH).show()
