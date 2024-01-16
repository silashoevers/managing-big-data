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
#                     'user.utc_offset'
#                     'user_count',
#                     'ts',
#                     'hour',
#                     'minute',
#                     'second',
#		      'time_bucket')

#receives df with filtered tweets and:
#   1) Checks number of spelling mistakes in each tweet
#   2) Calculates spelling mistakes as % of words in the tweet

def process_spelling_mistakes(sparksession, df_time_added):
    # Todo
    return df_time_added

def main(sparksession, df_filtered_tweets):
    df_spelling_processed = process_spelling_mistakes(sparksession, df_filtered_tweets)
    return df_spelling_processed

#expected output df structure:
# ('user_id', 
#   'username', 
#   'tweet_id',
#   'spelling_mistakes',
#   'percentage_wrong',
#   'time_bucket',
#   'lang')
#
# Maybe we can keep the information such as 'hour', 'minute' etc. so we have to possibility to graph spelling mistakes throughout time or something later
