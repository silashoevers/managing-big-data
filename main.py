import load_twitter
import filter_tweets
import process
import visualise_analyse

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# Set loglevel to WARN to reduce spam
spark.sparkContext.setLogLevel('WARN')

#source files to use
PATHS = '/data/doina/Twitter-Archive.org/2017-01/01/00/*.json.bz2'

#output folder path
OUTPUT = '/project' 

#load the paths
print(f'Loading tweet data from {PATHS}')
df_loaded = load_twitter.get_relevant_tweets_for_day(spark, PATHS)

#filter users we need
print('Filtering tweets')
df_filtered = filter_tweets.main(spark,df_loaded)

#process the filtered data
df_processed = process.main(spark,df_filtered)

#visualise results
visualise_analyse.main(spark,df_processed,OUTPUT)

