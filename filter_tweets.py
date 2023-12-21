#expected input df structure:

#receives df with tweets and filters out the users and tweets where:
#   The user tweeted at least 3 times that day
#   Those tweets were at least 4 hours apart each
#   The tweets were in English or Dutch

def main(sparksession,all_tweets):
    # Filter for only the langs that we support
    supported_langs = ['nl', 'en']
    tweets = all_tweets.filter(col('lang').rlike('^' + '|'.join(supported_langs) + '$'))

    # Filter for only users that tweeted multiple times during the day (>3)
    user_tweet_counts = tweets.groupBy('user_id').count().filter(col('count') > 3)

    # TODO: Filter out the tweets that are not spread out enough through the day

    # Only take the tweets from users who tweeted enough times
    tweets_2 = tweets.join(user_tweet_counts, on='user_id')
    return tweets_2, user_tweets_count

#expected output df structure: