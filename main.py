import load_twitter
import filter_tweets
import process
import visualise

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

#source files to use
#PATHS = ""

#load the paths
df_loaded = load_twitter.get_relevant_tweets_for_day(spark, load_twitter.PATH)

#filter users we need
df_filtered = filter_tweets.main(df_loaded)

#process the filtered data
df_processed = process.main(df_filtered)

#visualise results
visualise.main(df_processed)
