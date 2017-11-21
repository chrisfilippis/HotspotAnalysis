from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from sklearn import preprocessing
import math as math
from SparkWrapper import *


def ceil_string_value(string_value, step):
    return find_ceil_value(float(string_value), step)


def find_ceil_value(lat, step):
    return math.ceil(lat / step)

# a = np.zeros((5, 5, 5))
# print a

# decimal
# places   degrees          distance
# -------  -------          --------
# 0        1                111  km
# 1        0.1              11.1 km
# 2        0.01             1.11 km
# 3        0.001            111  m
# 4        0.0001           11.1 m
# 5        0.00001          1.11 m
# 6        0.000001         11.1 cm
# 7        0.0000001        1.11 cm
# 8        0.00000001       1.11 mm


step_lat = 0.01
step_lon = 0.01
csv_file_path = "C:\Users\cfilip09\Desktop\d\\randomdata_small.csv"
wrapper = SparkWrapper()

initSource = wrapper.load_rdd(path=csv_file_path)

rdd = initSource \
    .map(lambda s: (int(s[0]), ceil_string_value(s[1], step_lat), ceil_string_value(s[2], step_lon), int(s[3])))

print "csv"
print rdd.collect()
