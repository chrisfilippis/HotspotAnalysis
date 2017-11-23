from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from sklearn import preprocessing
import operator
import math as math

app_name = "Hot spot app"
master = "local"


def ceil_string_value(string_value, step):
    return find_ceil_value(float(string_value), step)


def find_ceil_value(lat, step):
    return math.ceil(lat / step)


def get_spark_session():
    return SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()


def get_spark_config():
    return SparkConf()\
        .setAppName(app_name)\
        .setMaster(master)


def get_spark_context():
    configuration = get_spark_config()
    return SparkContext.getOrCreate(conf=configuration)


# RDD.persists()
# dic = wrapper.get_spark_context().parallelize(rdd)
# collect() == collect all rdd data in a computers memory
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


sc = get_spark_context()
step_lat = 0.01
step_lon = 0.01
csv_file_path = "C:\Users\cfilip09\Desktop\d\\data.sample"
# csv_file_path = "C:\Users\cfilip09\Desktop\d\\bigdata.sample"

initSource = sc.textFile(csv_file_path)\
    .map(lambda line: line.split(" "))\

print "----------------"

rdd = initSource \
    .map(lambda s: (int(s[0]), ceil_string_value(s[2], step_lat), ceil_string_value(s[3], step_lon), int(s[1])))\
    .map(lambda s: (str(int(s[1])) + '_' + str(int(s[2])), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda x: x[1] > 1) \

for i in rdd.top(200, key=lambda x: x[1]):
    print i

print "csv"
