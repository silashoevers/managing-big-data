from scipy.stats import wilcoxon
from pyspark.sql.functions import avg
import matplotlib.pyplot as plt
import numpy as np
from itertools import combinations
from functools import reduce
import pyspark

time_buckets = ["morning", "afternoon", "evening", "night"]


def create_df_avg_dict(df_processed_tweets):
    # Spits out: {bucket: user_id, avg_mistake_ratio_bucket}

    # Per pair of timeframes, perform Wilcoxon signed-rank test on average mistake ratio between timeframes of same user

    # Split tweets from different time buckets into different dataframes
    df_morning = df_processed_tweets.filter(df_processed_tweets.time_bucket == "morning")
    df_afternoon = df_processed_tweets.filter(df_processed_tweets.time_bucket == "afternoon")
    df_evening = df_processed_tweets.filter(df_processed_tweets.time_bucket == "evening")
    df_night = df_processed_tweets.filter(df_processed_tweets.time_bucket == "night")

    df_morning_avg = df_morning.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_morning"))
    # TEMP debugging
    print(df_morning_avg.count())
    df_afternoon_avg = df_afternoon.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_afternoon"))
    df_evening_avg = df_evening.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_evening"))
    df_night_avg = df_night.groupBy('user_id').agg(avg('mistake_ratio').alias("avg_mistake_ratio_night"))

    # Simple test of average of averages of users in time bucket
    # morning_avg = df_morning_avg.agg({"avg_mistake_ratio_morning": "avg"}).collect()[0][0]
    # afternoon_avg = df_afternoon_avg.agg({"avg_mistake_ratio_afternoon": "avg"}).collect()[0][0]
    # evening_avg = df_evening_avg.agg({"avg_mistake_ratio_evening": "avg"}).collect()[0][0]
    # night_avg = df_night_avg.agg({"avg_mistake_ratio_night": "avg"}).collect()[0][0]

    # Create graph
    # averages = [morning_avg, afternoon_avg, evening_avg, night_avg]
    
    # fig, ax = plt.subplots()
    # bar_container = ax.bar(time_buckets, averages)
    # ax.set(ylabel='average of averaged ratio', title='Spelling mistakes per time bucket on 2017-01-01', ylim=(0,1))
    # ax.bar_label(bar_container, fmt='{:,.0f}')

    # plt.savefig("./output/bar_chart_averages.png")


    # Construct dictionary with shape {"time_bucket": df_time_bucket_avg}
    dict_df_avg = {
            "morning": df_morning_avg,
            "afternoon": df_afternoon_avg,
            "evening": df_evening_avg,
            "night": df_night_avg,
        }

    return dict_df_avg
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
def main(sparksession,resultPaths,outputPathFolder):


    days = []

    for result in resultPaths:
        df_processed_tweets = sparksession.read.parquet(result)
        days.append(create_df_avg_dict(df_processed_tweets))

    # Wilcoxon test between time buckets
    time_bucket_combinations = combinations(time_buckets, 2)

    for bucket_1, bucket_2 in time_bucket_combinations:
        all_days_same_buckets = []
        for day in days:
            df_1, df_2 = day[bucket_1], day[bucket_2]
            df_joined = df_1.join(df_2, 'user_id')
            all_days_same_buckets.append(df_joined)

        # Stack all dataframes of different days but same bucket comparison on top of each other
        # Based on this stackoverflow thread: https://stackoverflow.com/questions/74728180/union-list-of-pyspark-dataframes
        dfs_unioned = reduce(pyspark.sql.dataframe.DataFrame.unionByName, all_days_same_buckets)
        diff = [row[f'avg_mistake_ratio_{bucket_1}'] - row[f'avg_mistake_ratio_{bucket_2}'] for row in dfs_unioned.collect()]
        res = wilcoxon(diff)
        print(f"Statistics for {bucket_1} and {bucket_2} are {res.statistic} & {res.pvalue} with a total of {dfs_unioned.count()} rows considered")

    return

#expected output:
#write analysis results to a file