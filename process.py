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

from pyspark.sql.functions import col,when
import nltk
from nltk.metrics.distance import edit_distance
import os

# Make a df with encountered mistakes by language to make spell checking faster over time
# This also keeps track of how often a specific mispelling is used
    # structure: {language,spelling,[distance,count]}    
    # language   - String    - the language tag
    # spelling   - String    - this specific spelling of the word
    # distance   - int       - number of mistakes (based on [] distance between this spelling and closest known word)
    # count      - int       - number of times this mistake has been encountered, minimum of 1
    # word       - [String]  - registered spelling of the misspelled words (also named actual)
    # dist_count - (int,int) - contains a tuple of distance and count
COLUMNS = ['lang_id','spelling','dist_count_word']
df_mistakes_known = df_mistakes_known = sparksession.createDataFrame([('','',(0,0,[]))],COLUMNS)

# Correct word spelling lists
# Dutch word list source: https://github.com/OpenTaal/opentaal-wordlist
snum=os.getlogin()
list_schema = ['words']

df_dutch_words = sparksession.read.schema(list_schema).text("/user/"+snum"/wordlists/nl-words.txt")
# English word list taken from the nltk corpus
nltk.download('words')
from nltk.corpus import words #English words
df_english_words = sparksession.createDataFrame(list_schema,words.words())
#TODO check language tags accuracy
correct_words = {
        'en': df_english_words.ref,
        'nl': df_dutch_words.ref
}

def spell_check_word(language_code, word,sparksession):
    #check if the word is in the known mistakes
    global df_mistakes_known
    global list_schema
    global correct_words
    df_word = df_mistakes_known.filter(df_mistakes_known.lang_id == language_code).filter(df_mistakes_known.spelling == word)
    dist = 0
    if df_word.isEmpty():
        #if it is not 
        #TODO figure out how to find the closest word 
        # source: https://www.geeksforgeeks.org/correcting-words-using-nltk-in-python/ 
        df_correct_words_language = correct_words.get(language_code)
        #find the closest word and determine the edit distance
        #determine the edit distance to each word (key=word, value=distance)
        #sort then filter the df entries by distance to get lists of words with the same shortest edit distance
        df_word_dists = df_correct_words_language.withColumn(edit_distance(word,col(list_schema[0])))
        #TODO possible words finding task apparently not small, make more efficient
        # maybe make df and split into columns by letter, then filter?
        possible_words = [(edit_distance(word, w),w) for w in correct_words if w[0]==word[0]] 
        (dist,actual) = sorted(possible_words,key=lambda t:t[0])[0]
        
        #TODO set max distance of word to classify as different word
        #TODO possible extension: determine count threshold to say 'accepted new spelling'
        if dist>0:
        #if distance > 0, add the new spelling to the df with value and count
            df_new_mistake = sparksession.createDataFrame([(language_code,word,(dist,1,actual))],COLUMNS)
            df_mistakes_known = df_mistakes_known.union(df_new_mistake)

    else:
        #if it is increase the associated count
        dist = df_word.first()['dist_count_word'][0]
        count = df_word.first()['dist_count_word'][1] + 1
        actual = df_word.first()['dist_count_word'][2]
        df_mistakes_known = df_mistakes_known.withColumn('dist_count_word',when(col("spelling")==word,(dist,count,actual)).otherwise(col('dist_count_word')))
        
    #return distance
    return dist


#Maps word spell checker over tweet text, then returns the percentage of words misspelled
def spell_check_tweet(language_code, text,sparksession):    
    #Turn text into rdd with each word and dist to correct spelling
    #text to rdd
    rdd_tweet_text = sparksession.sparkContext.paralellize(clean_text.split(' ')).map(lambda w: (w,0))
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
    df_cleaned_tweets = df_filtered_tweets.select(
        col('user_id'),
        col('username'),
        col('id').alias('tweet_id'),
        text_clean(col('text')).alias('clean_text'),
        col('ts'),
        col('hour'),
        col('minute'),
        col('second'),
        col('time_bucket'),
        col('lang')
        )
    # map spell checker for tweets over all tweets
    df_checked_tweets = df_cleaned_tweets.withColumn('')
    
    return df_filtered_tweets, df_mistakes_known

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
