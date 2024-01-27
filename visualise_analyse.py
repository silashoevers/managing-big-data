from scipy.stats import wilcoxon
from pyspark.sql.functions import avg


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
def main(sparksession,df_processed_tweets,df_mistakes,outputPathFolder):
    # Per pair of timeframes, perform Wilcoxon signed-rank test on average mistake ratio between timeframes of same user

    # Split tweets from different time buckets into different dataframes
    df_morning = df_processed_tweets.filter(df_processed_tweets.time_bucket == "morning")
    df_afternoon = df_processed_tweets.filter(df_processed_tweets.time_bucket == "afternoon")
    df_evening = df_processed_tweets.filter(df_processed_tweets.time_bucket == "evening")
    df_night = df_processed_tweets.filter(df_processed_tweets.time_bucket == "night")

    df_morning_avg = df_morning.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_morning"))
    df_afternoon_avg = df_afternoon.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_afternoon"))
    df_evening_avg = df_evening.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_evening"))
    df_night_avg = df_night.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_night"))

    # Simple test of average of averages of users in time bucket
    df_morning_avg.agg({"avg_mistake_ratio_morning": "avg"}).show()
    df_afternoon_avg.agg({"avg_mistake_ratio_afternoon": "avg"}).show()
    df_evening_avg.agg({"avg_mistake_ratio_evening": "avg"}).show()
    df_night_avg.agg({"avg_mistake_ratio_night": "avg"}).show()

    # Construct dictionary with shape {"time_bucket": df_time_bucket_avg}
    dict_df_avg = {
            "morning": df_morning_avg,
            "afternoon": df_afternoon_avg,
            "evening": df_evening_avg,
            "night": df_night_avg,
        }

    # Wilcoxon test between time buckets
    time_bucket_combinations = [("morning", "afternoon"), ("morning", "evening"), ("morning", "night"),
                                ("afternoon", "evening"), ("afternoon", "night"), ("evening", "night")]

    for bucket_1, bucket_2 in time_bucket_combinations:
        df_1, df_2 = dict_df_avg[bucket_1], dict_df_avg[bucket_2]
        df_joined = df_1.join(df_2, 'user_id')
        diff = [row[f'avg_mistake_ratio_{bucket_1}'] - row[f'avg_mistake_ratio_{bucket_2}'] for row in df_joined.collect()]
        res = wilcoxon(diff)  # We expect the median of the difference to be smaller than 0
        print(f"Statistics for {bucket_1} and {bucket_2} are {res.statistic} & {res.pvalue}")

    return df_processed_tweets, df_mistakes

#expected output:
#write analysis results to a file
