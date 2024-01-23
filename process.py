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

from pyspark.sql.functions import col,when,asc, udf
import nltk
from nltk.metrics.distance import edit_distance
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,StructField, IntegerType, DoubleType
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

df_mistakes_known = None
correct_words = None
#structure of df_mistakes_known
COLUMNS = ['lang_id','spelling','dist_count_word']
#name of the column with correctly spelled words
col_name = "words"
def get_correct_wordlists(spark):
    global correct_words
    global df_mistakes_known
    global COLUMNS
    global col_name
    df_mistakes_known = spark.createDataFrame([('','',(0,0,['']))],COLUMNS)
    
    # Correct word spelling lists
    # Dutch word list source: https://github.com/OpenTaal/opentaal-wordlist
    snum=os.getlogin()
    
    df_dutch_words = spark.read.text("/user/"+snum+"/wordlists/nl-words.txt").toDF(col_name)
    # English word list taken from the nltk corpus
    nltk.download('words')
    from nltk.corpus import words #English words
    df_english_words = spark.createDataFrame(words.words(),"string").toDF(col_name)
    #TODO check language tags accuracy
    #according to Doina the variable names are object references, thus small data, so this should work
    correct_words = {
    	'en': df_english_words,
    	'nl': df_dutch_words
    }


def spell_check_word(language_code, word):
    global df_mistakes_known
    global col_name
    global correct_words
    global COLUMNS
    #check if the word is in the known mistakes
    df_word = df_mistakes_known.filter(df_mistakes_known.lang_id == language_code).filter(df_mistakes_known.spelling == word)
    dist = 0
    if df_word.isEmpty():
        #if it is not 
        df_correct_words_language = correct_words.get(language_code)
        #find the closest word and determine the edit distance
        #determine the edit distance to each word (key=word, value=distance)
        #sort then filter the df entries by distance to get lists of words with the same shortest edit distance
        df_word_dists = df_correct_words_language.withColumn('dist',edit_distance(word,col(col_name))).sort(asc("dist"))
        #get the shortest edit distance
        dist = df_word_dists.first()['dist']
        #TODO possible extension: set max distance of word to classify as a word from a different language
        #TODO possible extension: determine count threshold to say 'accepted new spelling'
        if dist>0:
        #if shortest edit distance > 0, add the new spelling to the df with correct word list and count
            #make list of the closest words
            df_closest_dists = df_word_dists.filter(col('dist')==dist)
            col_actual = df_closest_dists.collect()
            actual = [x.words for x in col_actual]
            #add new spelling to the df
            # sparksession = SparkSession.builder.getOrCreate()
            # df_new_mistake = sparksession.createDataFrame([(language_code,word,(dist,1,actual))],COLUMNS)
            # df_mistakes_known = df_mistakes_known.union(df_new_mistake)

    else:
        #if it is increase the associated count
        dist = df_word.first()['dist_count_word'][0]
        count = df_word.first()['dist_count_word'][1] + 1
        actual = df_word.first()['dist_count_word'][2]
        df_mistakes_known = df_mistakes_known.withColumn('dist_count_word',when(col("spelling")==word,(dist,count,actual)).otherwise(col('dist_count_word')))
        
    #return the edit distance
    return dist


#Maps word spell checker over tweet text, then returns number and the percentage of words misspelled
def spell_check_tweet(language_code, text):    
    #Turn text into rdd with each word and dist to correct spelling
    #text to rdd of words
    # sparksession = SparkSession.builder.getOrCreate()
    # rdd_tweet_text_split = sparksession.sparkContext.parallelize(text.split(' '))
    # rdd_tweet_text = rdd_tweet_text_split.map(lambda w: (w,0))
    # #map spell check words
    # rdd_tweet_spell_dist = rdd_tweet_text.map(lambda t: (t[0],spell_check_word(language_code=language_code,word=t[0],sparksession=sparksession)))
    # #count the number of misspelled words, giving the tweet total
    # total_mistakes = rdd_tweet_spell_dist.filter(lambda t: t[1]>0).count()
    # percent = (total_mistakes/rdd_tweet_text.count())*100

    words = text.split('')
    checker = lambda w: spell_check_word(language_code=language_code,word=w)
    checked = map(checker,words)
    total_mistakes = len(checked.filter(lambda t: t[1]>0))
    percent = (total_mistakes/len(checked))*100
    return (total_mistakes,percent)

    
#TODO write cleaner
def text_clean(text):
    cleaned_text = text
    return cleaned_text

def main(sparksession,df_filtered_tweets):
    #initialise mistakes log
    get_correct_wordlists(sparksession)
    cleaner = udf(lambda t: text_clean(t),StringType())
    tweet_spell_udf = udf(lambda l,t:spell_check_tweet(l,t),StructType([StructField('0',IntegerType()),StructField('1',DoubleType())]))
    # clean text of tweets
    df_cleaned_tweets = df_filtered_tweets.withColumn('clean_text',cleaner("text"))
    # map spell checker for tweets over all tweets
    df_checked_tweets = df_cleaned_tweets.withColumn('mistakes_percent',tweet_spell_udf("lang","clean_text"))
    # reformat checked tweets into desired output
    df_formatted_tweets = df_checked_tweets.select(
		    col('user_id'),
		    col('username'),
		    col('id').alias('tweet_id'),
		    col('mistakes_percent')['0'].alias('spelling_mistakes'),
		    col('mistakes_percent')['1'].alias('precentage_wrong'),
		    col('ts'),
		    col('hour'),
		    col('minute'),
		    col('second'),
		    col('time_bucket'),
		    col('lang')
		    )
    df_formatted_tweets.show()
    df_mistakes_known.show()
    exit()
    return df_formatted_tweets, df_mistakes_known

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
#
# Maybe we can keep the information such as 'hour', 'minute' etc. so we have to possibility to graph spelling mistakes throughout time or something later
