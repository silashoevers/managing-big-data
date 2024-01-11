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

# Make a df with encountered mistakes by language to make spell checking faster over time
# This also keeps track of how often a specific mispelling is used
    # structure: {language,spelling,[distance,count]}    
    # language   - String    - the language tag
    # spelling   - String    - this specific spelling of the word
    # distance   - int       - number of mistakes (based on [] distance between this spelling and closest known word)
    # count      - int       - number of times this mistake has been encountered, minimum of 1
    # word       - String    - registered spelling of the misspelled word (also named actual)
    # dist_count - (int,int) - contains a tuple of distance and count
COLUMNS = ['lang_id','spelling','dist_count_word']
df_mistakes_known = df_mistakes_known = sparksession.createDataFrame([('','',(0,0,''))],COLUMNS)

# Correct word spelling lists
nltk.download('words')
from nltk.corpus import words #English words
# Dutch word list source: https://github.com/OpenTaal/opentaal-wordlist
#TODO add dutch word list
#TODO check language tags accuracy
rdd_correct_words = sparksession.sparkContext.parallelize(
    [
        ('en',words.words()),
        ('nl',[])
    ]
)

def spell_check_word(language_code, word,sparksession):
    #check if the word is in the known mistakes
    global df_mistakes_known
    df_word = df_mistakes_known.filter(df_mistakes_known.lang_id == language_code).filter(df_mistakes_known.spelling == word)
    dist = 0
    if df_word.isEmpty():
        #if it is not 
        #TODO figure out how to find the closest word (binary search by letter? since NLTK has bigrams)
        # source: https://www.geeksforgeeks.org/correcting-words-using-nltk-in-python/ 
        #find the closest word and determine the edit distance
        correct_words = rdd_correct_words.lookup(language_code)
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
    #TODO clean up text from hashtags and such
    
    #Turn text into rdd with each word and dist to correct spelling
    #text to rdd
    rdd_tweet_text = sparksession.sparkContext.paralellize(text.split(' ')).map(lambda w: (w,0))
    #map spell check words
    rdd_tweet_spell_dist = rdd_tweet_text.map(lambda t: (t[0],spell_check_word(language_code=language_code,word=t[0],sparksession=sparksession)))
    #count the number of misspelled words, giving the tweet total
    total_mistakes = rdd_tweet_spell_dist.filter(lambda t: t[1]>0).count()
    return (total_mistakes/rdd_tweet_text.count())*100
    


def main(sparksession,df_filtered_tweets):
    # TODO map spell checker for tweets over all tweets
    global df_mistakes_known 
    df_mistakes_known = sparksession.createDataFrame([('','',(0,0,''))],COLUMNS)

    return df_filtered_tweets, df_mistakes_known

#expected output df structure:
# ('user_id', 
#   'username', 
#   'tweet_id',
#   'spelling_mistakes',
#   'percentage_wrong',
#   'time_bucket',
#   'lang')
