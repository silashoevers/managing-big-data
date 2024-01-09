from pyspark.sql.functions import timestamp_seconds, col, hour, minute, second
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
#                     'user_count')

#receives df with filtered tweets and:
#   1) Checks number of spelling mistakes in each tweet
#   2) Calculates spelling mistakes as % of words in the tweet

from pyspark.sql.functions import col,when,asc
import nltk
from nltk.metrics.distance import edit_distance
import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Make a df with encountered mistakes by language to make spell checking faster over time
# This also keeps track of how often a specific mispelling is used
    # structure: {language,spelling,(distance,count,word)}    
    # language          - String                - the language tag
    # spelling          - String                - this specific spelling of the word
    # distance          - int                   - number of mistakes (based on [] distance between this spelling and closest known word)
    # count             - int                   - number of times this mistake has been encountered, minimum of 1
    # word              - [String]              - registered spelling of the misspelled words (also named actual)
    # dist_count_word   - (int,int,[String])    - contains a tuple of distance, count, and closest words
COLUMNS = ['lang_id','spelling','dist_count_word']
df_mistakes_known = df_mistakes_known = sparksession.createDataFrame([('','',(0,0,[]))],COLUMNS)

# Correct word spelling lists
# Dutch word list source: https://github.com/OpenTaal/opentaal-wordlist
snum=os.getlogin()
list_schema = ['words']

df_dutch_words = spark.read.schema(list_schema).text("/user/"+snum+"/wordlists/nl-words.txt")
# English word list taken from the nltk corpus
nltk.download('words')
from nltk.corpus import words #English words
df_english_words = spark.createDataFrame(list_schema,words.words())
#TODO check language tags accuracy
#according to Doina the variable names are object references, thus small data, so this should work
correct_words = {
        'en': df_english_words,
        'nl': df_dutch_words
}

def spell_check_word(language_code, word,sparksession):
    global df_mistakes_known
    global list_schema
    global correct_words
    #check if the word is in the known mistakes
    df_word = df_mistakes_known.filter(df_mistakes_known.lang_id == language_code).filter(df_mistakes_known.spelling == word)
    dist = 0
    if df_word.isEmpty():
        #if it is not 
        df_correct_words_language = correct_words.get(language_code)
        #find the closest word and determine the edit distance
        #determine the edit distance to each word (key=word, value=distance)
        #sort then filter the df entries by distance to get lists of words with the same shortest edit distance
        df_word_dists = df_correct_words_language.withColumn('dist',edit_distance(word,col(list_schema[0]))).sort(asc("dist"))
        #get the shortest edit distance
        dist = df_word_dists.first()['dist']
        #TODO possible extension: set max distance of word to classify as a word from a different language
        #TODO possible extension: determine count threshold to say 'accepted new spelling'
        if dist>0:
        #if shortest edit distance > 0, add the new spelling to the df with correct word list and count
            #make list of the closest words
            df_closest_dists = df_word_dists.filter(col('dist')==dist)
            col_actual = df_closest_dists.collect()
            actual = [for x in col_actual x.words]
            #add new spelling to the df
            df_new_mistake = sparksession.createDataFrame([(language_code,word,(dist,1,actual))],COLUMNS)
            df_mistakes_known = df_mistakes_known.union(df_new_mistake)

    else:
        #if it is increase the associated count
        dist = df_word.first()['dist_count_word'][0]
        count = df_word.first()['dist_count_word'][1] + 1
        actual = df_word.first()['dist_count_word'][2]
        df_mistakes_known = df_mistakes_known.withColumn('dist_count_word',when(col("spelling")==word,(dist,count,actual)).otherwise(col('dist_count_word')))
        
    #return the edit distance
    return dist


#Maps word spell checker over tweet text, then returns number and the percentage of words misspelled
def spell_check_tweet(language_code, text,sparksession):    
    #Turn text into rdd with each word and dist to correct spelling
    #text to rdd of words
    rdd_tweet_text = sparksession.sparkContext.paralellize(text.split(' ')).map(lambda w: (w,0))
    #map spell check words
    rdd_tweet_spell_dist = rdd_tweet_text.map(lambda t: (t[0],spell_check_word(language_code=language_code,word=t[0],sparksession=sparksession)))
    #count the number of misspelled words, giving the tweet total
    total_mistakes = rdd_tweet_spell_dist.filter(lambda t: t[1]>0).count()
    return (total_mistakes,(total_mistakes/rdd_tweet_text.count())*100)
    
#TODO write cleaner
def text_clean(text):
    cleaned_text = text
    return cleaned_text

def main(sparksession,df_filtered_tweets):
    #initialise mistakes log
    global df_mistakes_known 
    df_mistakes_known = sparksession.createDataFrame([('','',(0,0,''))],COLUMNS)
    # clean text of tweets
    df_cleaned_tweets = df_filtered_tweets.withColumn('clean_text',text_clean(col('text')))
    # map spell checker for tweets over all tweets
    df_checked_tweets = df_cleaned_tweets.withColumn('mistakes_percent',spell_check_tweet(col('clean_text')))
    # reformat checked tweets into desired output
     df_formatted_tweets = df_checked_tweets.withColumn(
        col('user_id'),
        col('username'),
        col('id').alias('tweet_id'),
        col('mistakes_percent').getItem(0).alias('spelling_mistakes'),
        col('mistakes_percent').getItem(1).alias('precentage_wrong'),
        col('ts'),
        col('hour'),
        col('minute'),
        col('second'),
        col('time_bucket'),
        col('lang')
        )
    return df_formatted_tweets, df_mistakes_known
def add_time_window(sparksession, df_filtered_tweets):
    # TODO move this to filter_tweets.py

    sparksession.conf.set("spark.sql.session.timeZone", "UTC")

    # Filter based on timestamp being not null
    df_filtered_tweets = df_filtered_tweets.filter(col('utc_offset').isNotNull())

    # Convert into local time timestamps
    df_timestamps = df_filtered_tweets.withColumn('ts', \
            timestamp_seconds(col('timestamp_ms') / 1e3 + col('utc_offset'))) 


    # Extract the hour
    df_timestamps = df_timestamps.withColumns({ 'hour': hour('ts'), 'minute': minute('ts'), 'second': second('ts') })

    # Add time-of-day as boolean flags
    df_timestamp_buckets = df_timestamps.withColumns( {'night': col('hour') < 6,
        'morning': (6 <= col('hour')) & (col('hour') < 12),
        'afternoon': (12 <= col('hour')) & (col('hour') < 18),
        'evening': 18 <= col('hour')})

    df_timestamp_buckets.sort('timestamp_ms', ascending=True).show()
    return df_timestamp_buckets

def process_spelling_mistakes(sparksession, df_time_added):
    # Todo
    return df_time_added

def main(sparksession, df_filtered_tweets):
    df_time_added = add_time_window(sparksession, df_filtered_tweets)
    df_spelling_processed = process_spelling_mistakes(sparksession, df_time_added)
    return df_spelling_processed

#expected output df structure:
# ('user_id', 
#   'username', 
#   'tweet_id',
#   'spelling_mistakes',
#   'percentage_wrong',
#   'ts'
#   'hour'
#   'minute'
#   'second'
#   'time_bucket'
#   'lang')

#testing
print("should be 0 :"+ str(spell_check_word('en',"five",spark)))
print("should be 1 :"+ str(spell_check_word('en',"fife",spark)))