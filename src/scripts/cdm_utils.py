import math
import datetime
import pyspark.sql.functions as F

def to_radians(gr):
    return gr*math.pi / 180

def find_distance(lat1, lon1, lat2, lon2):
    lat1 = to_radians(lat1)
    lat2 = to_radians(lat2)
    lon1 = to_radians(lon1)
    lon2 = to_radians(lon2)
    sin_pow1 = F.pow(F.sin((lat2 - lat1)/F.lit(2)), 2)
    sin_pow2 = F.pow(F.sin((lon2 - lon1)/F.lit(2)), 2)
    distance = 2 * 6371 * F.asin(F.sqrt(sin_pow1 + F.cos(lat1)*F.cos(lat2)* sin_pow2))
    return distance

def input_event_paths(base_path, date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(int(depth))]
