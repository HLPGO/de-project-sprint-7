from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
import sys, os
from cdm_utils import find_distance, input_event_paths

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

date = sys.argv[1]
depth = sys.argv[2]
base_path = sys.argv[3]

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

def first_cdm():
        paths = input_event_paths(base_path, date, depth)
        big_df = sql.read(paths)
        df_events = df_with_city(big_df, df_city)

        # преобразуем таблицу выделяем сквозные ключи
        df_events =     (df_events.withColumn('user_id', F.coalesce(F.col('message_from'), F.col('reaction_from'), F.col('user')))
                                .withColumn('event_time', F.coalesce(F.col('message_ts'), F.col('datetime')))
                                .withColumn('event_dt', F.date_trunc('event_dt', 'day'))
                        )
        # выделим отдельный датафрейм на act_city
        act_city =      (df_events.select('event_dt', 'event_time', 'user_id', 'event_id', F.col('city').alias('act_city'))
                        .withColumn("rank", F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc("event_dt"))))
                        .filter('rank' == 1)        
                        )
        # вспомогательная таблица для поиска путешествий
        tmp_table =     (
                        df_events
                        .withColumn("rank", F.row_number('*').over(Window.partitionBy('user_id', 'city').orderBy(F.desc("event_dt"))))
                        .withColumn('tmp_date', F.col('event_dt') - F.date_sub('rank'))
                        .groupBy('user_id', 'city', 'tmp_date')
                        .agg(F.count('event_dt').alias('days_in'), F.max('event_dt').alias('max_dt'))
                        )
        # домашний город
        home_city =     (tmp_table.select('event_dt', 'user_id', 'event_id', F.col('city').alias('home_city'))
                        .filter('days_in' > 27)
                        .withColumn("rank", F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc("max_dt"))))
                        .filter('rank' == 1)  
                        )
        travels =       (
                        tmp_table.select("user_id", "city")
                        .groupBy('user_id')
                        .agg(F.collect_list('city').alias("travel_array"), F.count('city').alias("travel_count")) 
                        )

        first_cdm = (act_city.join(travels, 'user_id', 'left')
                        .select('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', F.col('event_time').alias('TIME_UTC'))
                        .join(home_city, 'city', 'left')
                        .join(df_timezone, 'city', 'left')
                        .withColumn('local_time', F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone')))
        )

        first_cdm.write\
                .format('parquet')\
                .save('/user/pgoeshard/data/analytics/cdm/cdm1')
        return



