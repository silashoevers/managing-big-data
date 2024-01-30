import load_twitter
import filter_tweets
import process
import visualise_analyse
import os

import pandas as pd
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
mistakes_dir_prefix = f'/user/{os.getlogin()}/result/df_mistakes_2017_01_'

days = [f'{i:02d}' for i in range(1, 8)]
PATHS = [path_prefix + d + path_suffix for d in days]
RESULTS = [results_dir_prefix + d for d in days]

#output folder path
OUTPUT = './output' 
os.makedirs(OUTPUT, exist_ok=True)

tweet_counts = []

for path, result, day in zip(PATHS, RESULTS, days):

    # path for saving df_mistakes
    mistake_dir = mistakes_dir_prefix + day

    #load the paths
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    print(f'Loading tweet data from {path}')
    df_loaded = load_twitter.get_relevant_tweets_for_day(spark, path)

    #filter users we need
    print('Filtering tweets')
    num_tweets_prefilter = df_loaded.count()
    print(f'Number of tweets before filtering: {num_tweets_prefilter}')
    df_filtered = filter_tweets.main(spark,df_loaded)
    num_tweets_postfilter = df_filtered.count()
    print(f'Number of tweets after filtering: {num_tweets_postfilter}')

    #process the filtered data
    print('Processing tweet text')
    df_processed, df_mistakes = process.main(spark,df_filtered)
    num_tweets_processed = df_processed.count()
    print(f'Number of tweets after processing: {num_tweets_processed}')

    ## TEMP Store results on HDFS
    print(f'Storing intermediate results in {result}')
    df_processed.write.parquet(result, mode="overwrite")
    print(f'Storing df_mistakes in {mistake_dir}')
    df_mistakes.write.parquet(mistake_dir, mode="overwrite")

    tweet_counts.append(dict(day=day,
                             prefilter=num_tweets_prefilter,
                             postfilter=num_tweets_postfilter,
                             processed=num_tweets_processed))

# Save tweet counts as dataframe
df_tweet_counts = pd.DataFrame(tweet_counts)
print(df_tweet_counts)
df_tweet_counts.to_csv(OUTPUT + 'tweet_counts.csv')

visualise_analyse.main(spark,RESULTS,OUTPUT)

