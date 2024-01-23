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


def get_correct_wordlists(spark):
    # Correct word spelling lists
    # Dutch word list source: https://github.com/OpenTaal/opentaal-wordlist
    snum=os.getlogin()
    df_dutch_words = spark.read.text("/user/"+snum+"/wordlists/nl-words.txt").toDF("words")
    # English word list taken from the nltk corpus
    nltk.download('words')
    from nltk.corpus import words #English words
    df_english_words = spark.createDataFrame(words.words(),"string").toDF("words")
    #TODO check language tags accuracy
    #according to Doina the variable names are object references, thus small data, so this should work
    correct_words = {
    	'en': df_english_words,
    	'nl': df_dutch_words
    }
    return correct_words


def spell_check_word(word,correct_words):
    dist = 0
    #find the closest word and determine the edit distance
    #determine the edit distance to each word (key=word, value=distance)
    #sort then filter the df entries by distance to get lists of words with the same shortest edit distance
    df_word_dists = correct_words.withColumn('dist',edit_distance(word,col("words"))).sort(asc("dist"))
    #get the shortest edit distance
    dist = df_word_dists.first()['dist']
    #TODO possible extension: set max distance of word to classify as a word from a different language
    #TODO possible extension: determine count threshold to say 'accepted new spelling'
    #return the edit distance
    return dist


#Maps word spell checker over tweet text, then returns number and the percentage of words misspelled
def spell_check_tweet(language_code, text,correct_words):    
    #split the text into individual words
    words = text.split('')
    #get the list of correct words in the right language
    df_correct_words_language = correct_words.get(language_code)
    #make a mapable function for checking word spelling
    checker = lambda w: spell_check_word(word=w,correct_words=df_correct_words_language)
    #map over the list of words to get a list of edit distances to a correct word
    checked = map(checker,words)
    #find the number of mispelled words by filtering to edit distances over 0
    total_mistakes = len(checked.filter(lambda t: t>0))
    #calculate percentage of words in tweet misspelled
    percent = (total_mistakes/len(checked))*100
    return (total_mistakes,percent)

def spell_check_rdd(df_correct_words,df_tweets):
    #convert to RDD
    #flatmap value split(' ') to get separate words
    #cross product with correct words
    #add distance
    #reduce by id,word then only keep lowest distance value
    #reduce by id to determine number of mispelled words and %
    #return a df from this rdd
    return df_checked_tweets
    
#TODO write cleaner
def text_clean(text):
    cleaned_text = text
    return cleaned_text

def main(sparksession,df_filtered_tweets):
    #initialise the correct word lists
    correct_words = get_correct_wordlists(sparksession)
    #define udfs for cleaning an spell checking tweets
    cleaner = udf(lambda t: text_clean(t),StringType())
    tweet_spell_udf = udf(lambda l,t:spell_check_tweet(l,t, correct_words),StructType([StructField('0',IntegerType()),StructField('1',DoubleType())]))
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
    exit()
    return df_formatted_tweets

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
