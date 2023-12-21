import load_twitter
import filter_tweets
import process
import visualise

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

#source files to use
PATHS = '/data/doina/Twitter-Archive.org/2017-01/01/*/*.json.bz2'

#load the paths
df_loaded = load_twitter.get_relevant_tweets_for_day(spark, PATHS)

#filter users we need
df_filtered = filter_tweets.main(spark,df_loaded)

#process the filtered data
df_processed = process.main(spark,df_filtered)

#visualise results
visualise.main(spark,df_processed)
