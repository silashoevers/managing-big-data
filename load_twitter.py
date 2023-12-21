#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()

path = '/data/twitterNL/201502/20150228-20.out.gz'

df1 = spark.read.json(path)

df1.printSchema()

df2 = df1.select('id',
                 'text',
                 'user.lang',
                 col('user.id').alias('user_id'),
                 col('user.name').alias('username'),
                 'user.verified',
                 'timestamp_ms',
                 'user.time_zone',
                 'user.utc_offset')

supported_langs = ['nl', 'en']
df3 = df2.filter(col('lang').rlike('^' + '|'.join(supported_langs) + '$'))


