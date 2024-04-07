import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys
from cdm_utils import find_distance, input_event_paths
from pyspark.sql import SparkSession, DataFrame

date = sys.argv[1]
depth = sys.argv[2]
base_path = sys.argv[3]
output_base_path = sys.argv[4]

conf = SparkConf().setAppName("Some_cool_job")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

df_city = sql.read.csv('/user/pgoeshard/data/analytics/city')
df_timezone = sql.read.csv('/user/pgoeshard/data/analytics/timezone')

def df_with_city(df_events: DataFrame, df_city: DataFrame) -> DataFrame:
    df_with_city =  (df_events                                                     
                    .withColumn('lat_1', F.col('lat'))        
                    .withColumn('lon_1', F.col('lon'))        
                    .withColumn('event_id', F.monotonically_increasing_id())      
                    .crossJoin(df_city
                            .withColumn('lat_2', F.col('lat'))
                            .withColumn('lon_2', F.col('lon'))
                            )
                    .withColumn('distance', find_distance(F.col('lat_1'), F.col('lon_1'), F.col('lat_2'), F.col('lon_2')))
                    .withColumn("rank", F.row_number().over(Window.partitionBy("event_id").orderBy(F.desc("distance"))))
                    .filter('rank' == 1)
                    )
    return df_with_city

def third_cdm() -> DataFrame:
    events = sql.read.parquet('/user/pgoeshard/data/events') 
    ev_with_city = df_with_city(events, df_city)
    subs = (ev_with_city.select('user', 'channel')
                     .filter('event == subscription'))
    candidates = (subs.alias('1').join(subs.alias('2'), '1.channel == 2.channel and 1.user != 2.user')
                       .distinct()
                  )
    contacters = (ev_with_city.select('message_from', 'message_to')
                            .union(ev_with_city.select('message_to', 'message_from'))
                            .distinct()
    )
    two_conditions = candidates.join(contacters, 'user_id', 'left_anti')

    tmp_table = (ev_with_city.withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("event_time"))))
                            .filter('rank' == 1)
    )    

    users_not_met = (two_conditions.select('user_left', 'user_right')
                     .join(tmp_table, 'user_left = user_id', 'left')
                     .join(tmp_table, 'user_right = user_id', 'left')
                    .withColumn('dist', find_distance(F.col('lat_1'), F.col('lon_1'), F.col('lat_2'), F.col('lon_2')))
                    .filter('dist' < 1)
                    .withColumn('processed_dttm', F.current_date())
                    .withColumn('zone_id', F.col('city'))
                    .withColumn('local_time', F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone')))
    )
    users_not_met.write\
        .format('parquet')\
        .save('/user/pgoeshard/data/analytics/cdm/cdm3')
    return

