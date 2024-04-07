from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys
from pyspark.sql import SparkSession, DataFrame
from cdm_utils import find_distance, input_event_paths

date = sys.argv[1]
depth = sys.argv[2]
base_path = sys.argv[3]
output_base_path = sys.argv[4]

conf = SparkConf().setAppName("Some_cool_job")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

df_city = sql.read.csv('/user/pgoeshard/data/analytics/city')

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
    

def sec_cdm() -> DataFrame:
    paths = input_event_paths(base_path, date, depth)
    big_df = sql.read(paths)
    df_events = df_with_city(big_df, df_city)

    # преобразуем таблицу выделяем сквозные ключи
    df_events =     (df_events.withColumn('user_id', F.coalesce(F.col('message_from'), F.col('reaction_from'), F.col('user')))
                            .withColumn('event_time', F.coalesce(F.col('message_ts'), F.col('datetime')))
                            .withColumn('event_dt', F.date_trunc('event_dt', 'day'))
                            .withColumn("reg", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("event_time"))))
                    )

    cdm2     =       (df_events.select('city_id', 'event_id', 'event_dt')
                            .withColumn("month", F.date_trunc('event_dt', 'month'))
                            .withColumn('week', F.date_trunc('event_dt', 'week'))
                            .withColumn('message', F.when('event_type == message', 1).otherwise(0))
                            .withColumn('reaction', F.when('event_type == reaction', 1).otherwise(0))
                            .withColumn('subscription', F.when('event_type == subscription', 1).otherwise(0))
                            .withColumn('registration', F.when('reg == 1', 1).otherwise(0))
                            .groupBy('week', 'city', 'month')
                            .agg(F.sum('message'), F.sum('reaction'), F.sum('subscription'), F.sum('registration'))
                            .withColumn('month_message',  F.sum('message').over(Window.partitionBy('month')))
                            .withColumn('month_reaction',  F.sum('reaction').over(Window.partitionBy('month')))
                            .withColumn('month_subscription',  F.sum('suscription').over(Window.partitionBy('month')))
                            .withColumn('month_user',  F.sum('user').over(Window.partitionBy('month')))
                    )

    cdm2.write\
            .format('parquet')\
            .save('/user/pgoeshard/data/analytics/cdm/agg_cdm')
    return

