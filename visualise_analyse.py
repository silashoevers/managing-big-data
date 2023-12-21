#expected input df structure:

#receives a df of processed tweets then analyses and visualises the results
#   1) Calculate average % spelling mistakes per time window per user
#       Time windows: 
#           Morning     (06:00-12:00)
#           Afternoon   (12:00-18:00)
#           Evening     (18:00-00:00)
#           Night       (00:00-06:00)
#   2) Do relevant statistics tests with paired samples per user throughout different time windows
#   3) Generate relevant visualisations of results
def main(sparksession,df_processed_tweets,outputPathFolder):
    #TODO write actual code
    return df_processed_tweets

#expected output:
#write analysis results to a file
