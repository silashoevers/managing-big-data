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

# Where to load data
path_prefix='/data/doina/Twitter-Archive.org/2017-01/'
path_suffix = '/*/*.json.bz2'

# Where to store intermediate results
results_dir_prefix = f'/user/{os.getlogin()}/result/df_processed_2017_01_'

days = ['0' + str(i) for i in range(1, 8)]
PATHS = [path_prefix + d + path_suffix for d in days]
RESULTS = [results_dir_prefix + d for d in days]

#output folder path
OUTPUT = '/output' 

for path, result in zip(PATHS, RESULTS):

    #load the paths
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    print(f'Loading tweet data from {path}')
    df_loaded = load_twitter.get_relevant_tweets_for_day(spark, path)

    #filter users we need
    print('Filtering tweets')
    print(f'Number of tweets before filtering: {df_loaded.count()}')
    df_filtered = filter_tweets.main(spark,df_loaded)
    print(f'Number of tweets after filtering: {df_filtered.count()}')

    #process the filtered data
    print('Processing tweet text')
    df_processed, df_mistakes = process.main(spark,df_filtered)
    print(f'Number of tweets after processing: {df_processed.count()}')

    # TEMP Store results on HDFS
    print(f'Storing intermediate results in {result}')
    df_processed.write.parquet(result, mode="overwrite")

# TEMP Load results
visualise_analyse.main(spark,RESULTS,OUTPUT)

