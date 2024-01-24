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
from itertools import repeat


def get_correct_wordlists(spark):
    # Correct word spelling lists
    # Dutch word list source: https://github.com/OpenTaal/opentaal-wordlist
    snum=os.getlogin()
    df_dutch_words = spark.read.text("/user/"+snum+"/wordlists/nl-words.txt").toDF("words")
    # English word list taken from the nltk corpus
    from nltk.corpus import words #English words
    df_english_words = spark.createDataFrame(words.words(),"string").toDF("words")
    #TODO check language tags accuracy
    #according to Doina the variable names are object references, thus small data, so this should work
    correct_words = {
    	'en': df_english_words,
    	'nl': df_dutch_words
    }
    return correct_words


def spell_check_rdd(correct_words,df_tweets, max_length_diff=1, max_edit_dist=2, min_word_len=4):
    #convert to RDD
    #flatmap value split(' ') to get separate words
    #cross product with correct words
    #add distance
    #reduce by id,word then only keep lowest distance value
    #reduce by id to determine number of mispelled words and %
    #return a df from this rdd

    ### Turn correct_words from dataframes into RDDs
    rdd_en_words = correct_words['en'].rdd.map(lambda w: ('en', w.words))
    rdd_nl_words = correct_words['nl'].rdd.map(lambda w: ('nl', w.words))
    rdd_correct_words = rdd_en_words.union(rdd_nl_words) \
            .distinct()  # Word list should not contain duplicates

    ### Convert df_tweets into RDD
    # tweet_id, (lang, text)
    rdd_tweets = df_tweets.rdd.map(lambda row: (row.id, (row.lang, row.text)))

    ### Split text into words
    # tweet_id, (lang, word)
    rdd_tweet_words = rdd_tweets.flatMapValues(lambda t: zip(repeat(t[0]), t[1].split())) \
	        #.filter(lambda t: t[1][1].startswith('reme'))
    
    # lang, (tweet_id, word)
    rdd_tweet_words_langfirst = rdd_tweet_words.map(lambda t: (t[1][0], (t[0], t[1][1])))

    ### Cartesian product with all correct_words for the given language
    # (tweet_id, word), correct_word
    rdd_words_correctproduct = rdd_tweet_words_langfirst.join(rdd_correct_words)\
		.map(lambda t: ((t[1][0][0], t[1][0][1]), t[1][1]))

    def filter_word_length(tweet_word, correct_word):
        length_diff = len(correct_word) <= len(tweet_word) + max_length_diff
        tweet_word_length = min_word_len <= len(tweet_word)
        return length_diff and tweet_word_length

    ### Don't consider words that are too small
    rdd_words_correctproduct = rdd_words_correctproduct.filter(lambda t: filter_word_length(t[0][1], t[1]))

    def filter_word_edit_dist(tweet_word, correct_word, edit_distance):
        """
        TODO take into account something like the tweet_word length?
        Since right now even a 2-letter word will be close enough to all correct 3 letter words
        """
        return edit_distance <= max_edit_dist

    ### Calculate all edit distances between words and correct words
    ### and consider only words with a low edit distance
    # (tweet_id, word), (correct_word, edit_distance)
    rdd_spellcheck_dist = rdd_words_correctproduct.map(lambda t: ((t[0][0], t[0][1]), (t[1], edit_distance(t[0][1], t[1]))), preservesPartitioning=True) \
	        .filter(lambda t: filter_word_edit_dist(t[0][1], t[1][0], t[1][1]))
    #[print(r) for r in rdd_spellcheck_dist.take(20)]  # Doesn't take _that_ long to run

    def smaller_edit_dist(rdd_row_a, rdd_row_b):
        """Reduction for finding all the correct spelling of a word with lowest edit dist"""
        edit_dist_a, words_a = rdd_row_a
        edit_dist_b, words_b = rdd_row_b

        if edit_dist_a < edit_dist_b:
            return edit_dist_a, words_a
        elif edit_dist_a > edit_dist_b:
            return edit_dist_b, words_b
        return edit_dist_a, [*words_a, *words_b]

    print('Reducing to find smallest edit dist per word. THIS TAKES A WHILE')
    rdd = rdd_spellcheck_dist.mapValues(lambda v: (v[1], [v[0]])) \
            .reduceByKey(smaller_edit_dist)  # Expensive!

    [print(r) for r in rdd.take(200)]

    print('Spelling mistake % calculation not yet implemented')
    exit()
    return df_tweets
    
#TODO write cleaner
def text_clean(text):
    cleaned_text = text
    return cleaned_text

def main(sparksession,df_filtered_tweets):
    #initialise the correct word lists
    correct_words = get_correct_wordlists(sparksession)
    #define udfs for cleaning an spell checking tweets
    cleaner = udf(lambda t: text_clean(t),StringType())
    df_cleaned_tweets = df_filtered_tweets.withColumn('clean_text',cleaner("text"))

    spellmistakes = spell_check_rdd(correct_words, df_cleaned_tweets)
    exit()
    


   
    """
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
    """

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
