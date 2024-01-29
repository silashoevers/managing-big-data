import load_twitter
import filter_tweets
import process
import visualise_analyse
import os

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = './venv/bin/python'
spark = SparkSession.builder.config('spark.archives', 'pyspark_venv.tar.gz#venv').getOrCreate()
# Set loglevel to WARN to reduce spam
spark.sparkContext.setLogLevel('WARN')

#source files to use
PATHS = ['/data/doina/Twitter-Archive.org/2017-01/01/*/*.json.bz2', '/data/doina/Twitter-Archive.org/2017-01/02/*/*.json.bz2']

RESULTS = [f'/user/{os.getlogin()}/result/df_processed_2017_01_01', f'/user/{os.getlogin()}/result/df_processed_2017_01_02']

#output folder path
OUTPUT = '/output' 

for path, result in zip(PATHS, RESULTS):

    #load the paths
    print(f'Loading tweet data from {path}')
    df_loaded = load_twitter.get_relevant_tweets_for_day(spark, path)

    #filter users we need
    print('Filtering tweets')
    df_filtered = filter_tweets.main(spark,df_loaded)

    #process the filtered data
    print('Processing tweet text')
    df_processed, df_mistakes = process.main(spark,df_filtered)

    # TEMP Store results on HDFS
    df_processed.write.parquet(result, mode="overwrite")

# TEMP Load results

visualise_analyse.main(spark,RESULTS,OUTPUT)

