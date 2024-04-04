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

#1 витрина
#user_id act_city home_city travel_count travel_array local_time

def third_cdm():
    df_user = sql.read.parquet('/user/pgoeshard/data/analytics/cdm/users_home_city')
    big_df_user = df_user.join(df_user, 'subscription_channel', 'cross')\
                        .withColumn('distance', F.pow(F.sin((F.col('lat_1') - F.col('lat_2'))/F.lit(2))))\
                        .where('distance' < 1000)\
                        .withColumn('processed_dttm', F.current_date())
                        
    users_not_met = big_df_user
    users_not_met.sql.write\
        .format('parquet')\
        .save('/user/pgoeshard/data/analytics/cdm/cdm3')
    return


#travel_array
#ранжировать по городам оставить только первую запись
#расчет travel_count
#посчитать строки в  travel_array
#местное время
#F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone'))
#2 витрина
#недельные, месячные агрегации
#3 витрина
#9 перемножить всех пользователей внутри каждого города
#10 найти расстояния между ними, выбрать тех у кого меньше 1 км
#11 проверить на наличие связей между ними до этого
#12 проверить подписку на 1 канал и более