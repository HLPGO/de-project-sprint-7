#1 умножить города на все события
#2 посчитать все расстояния всех событий между всеми городами
#3 отранжировать расстояние по возрастанию
#4 оставить только самый ближний город
#5 посчитать количество событий в городах каждого пользователя за 27 дней
#6  отранжировать по убыванию
#7 оставить самый активный город для каждого пользователя - это домашний город
#8 город последнего события - актуальный город


import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys

date = sys.argv[1]
depth = sys.argv[2]
base_path = sys.argv[3]
output_base_path = sys.argv[4]

conf = SparkConf().setAppName("Some_cool_job")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

df_city = sql.read.csv('/user/pgoeshard/data/analytics/city')
df_timezone = sql.read.csv('/user/pgoeshard/data/analytics/timezone')
event_type = ['message', 'reaction', 'subscription', 'user']

def df_with_city(df_events, df_city, event_id):
    big_df = df_events.join(df_city, 'cross')
    big_df = big_df.withColumn('distance', F.pow(F.sin((F.col('lat_1') - F.col('lat_2'))/F.lit(2))))
    big_df_rank = big_df.withColumn("rank", F.row_number().over(Window.partitionBy(f"event.{event_id}", ).orderBy(F.desc("distance"))))
    df_with_city = big_df_rank.groupBy(f'event.{event_id}').agg(F.first("distance"))
    return df_with_city
    
def input_event_paths(base_path, date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(int(depth))]

def sec_cdm():
    paths = input_event_paths(base_path, date, depth)
    big_df = sql.read(paths)
    df_message = df_with_city(big_df, df_city, 'message_id')
    df_message = df_message.withColumn('month', F.date_trunc('message_ts', 'month'))\
                            .withColumn('week', F.date_trunc('message_ts', 'week'))\
                            .withColumn('day', F.date_trunc('message_ts', 'day'))\
                            .groupBy('city', 'month', 'week', 'day')\
                            .agg((F.count('month', 'week', 'day')))
    df_message.sql.write\
            .format('parquet')\
            .save('/user/pgoeshard/data/analytics/cdm/agg_cdm')
    return

