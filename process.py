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

from textblob import TextBlob
from pyspark.sql.functions import col,when
from operator import add

# Make a df with encountered mistakes by language to make spell checking faster over time
# This also keeps track of how often a specific mispelling is used
    # structure: {language,spelling,[distance,count]}    
    # language   - String    - the language tag
    # spelling   - String    - this specific spelling of the word
    # distance   - int       - number of mistakes (based on [] distance between this spelling and closest known word)
    # count      - int       - number of times this mistake has been encountered, minimum of 1
    # dist_count - (int,int) - contains a tuple of distance and count
COLUMNS = ['lang_id','word','dist_count']
df_mistakes_known = sparksession.createDataFrame([],COLUMNS)

def spell_check_word(language_code, word,sparksession):
    #check if the word is in the known mistakes
    df_word = df_mistakes_known.filter(df_mistakes_known.lang_id == 'nl').filter(df_mistakes_known.word == word)
    dist = 0
    if df_word.isEmpty():
        #if it is not 
        #TODO figure out how to find the closest word (binary search by letter? since NLTK has bigrams)
        #find the closest word

        #determine the distance
        #TODO pick distance measure
        if dist>0:
        #if distance > 0, add the new spelling to the df with value and count
            df_new_mistake = sparksession.createDataFrame([(language_code,word,(dist,1))],COLUMNS)
            df_mistakes_known = df_mistakes_known.union(df_new_mistake)

    else:
        #if it is increase the associated count
        dist = df_word.first()['dist_count'][0]
        count = df_word.first()['dist_count'][1] + 1
        df_mistakes_known = df_mistakes_known.withColumn('dist_count',when(col("word")==word,(dist,count)).otherwise(col('dist_count')))
        
    #return distance and known mistakes df
    return dist

#Maps word spell checker over tweet text, then returns the total of all spelling distances
def spell_check_tweet(language_code, text,sparksession):
    #Turn text into rdd with each word and dist to correct spelling
    #text to rdd
    rdd_tweet_text = sparksession.sparkContext.paralellize(text.split(' ')).map(lambda w: (w,0))
    #map spell check words
    rdd_tweet_spell_dist = rdd_tweet_text.map(lambda t: (t[0],spell_check_word(language_code=language_code,word=t[0],sparksession=sparksession)))
    #Sum up the total dist of all words to their correct spelling, giving the tweet total
    total_dist = rdd_tweet_spell_dist.map(lambda t: t[0][0]).reduce(add)

    return total_dist
    


def main(sparksession,df_filtered_tweets):
    # TODO map spell checker for tweets over all tweets
    df_mistakes_known = sparksession.createDataFrame([],COLUMNS)
    
    return df_filtered_tweets, df_mistakes_known

#expected output df structure:
# ('user_id', 
#   'username', 
#   'tweet_id',
#   'spelling_mistakes',
#   'percentage_wrong',
#   'time_bucket',
#   'lang')
