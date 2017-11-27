from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from scipy.spatial import distance
import pysal.esda
from pyspark.sql import SQLContext
from sklearn import preprocessing
import operator
import math as math

app_name = "Hot spot app"
master = "local"


def minutes_batch(unix_timestamp, minTime, step):
    step_seconds = 60 * step
    difference = unix_timestamp - minTime;
    return int(difference / step_seconds)


def ceil_val(lat, step):
    return math.ceil(divide_val(lat, step))


def divide_val(lat, step):
    return lat / step


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


def get_weight(point):
    return distance.euclidean((0, 0, 0), point)


def print_formatted(points_to_print, top=20, key=None):

    if key is None:
        for pri in points_to_print.top(top):
            print pri
    else:
        for pri in points_to_print.top(top, key=key):
            print pri

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
step_time = 10
csv_file_path = "C:\Users\cfilip09\Desktop\d\\data.sample"
# csv_file_path = "C:\Users\cfilip09\Desktop\d\\bigdata.sample"

initSource = sc.textFile(csv_file_path)\
    .map(lambda line: line.split(" "))\

# .map(lambda s: (str(int(s[1])) + '_' + str(int(s[2])) + '_' + str(s[0]), 1)) \

# string: time, string: lat, string: lon, string: id
source = initSource \
    .map(lambda s: (int(s[0]), float(s[2]), float(s[3]), int(s[1])))

# find the minimum date
min_time = source \
    .map(lambda s: (s[0])) \
    .min()

# find the number of cells
number_of_cells = source.count()

# int: time, int: lat, int: lon, float: xi, int: id
structured_data = source \
    .map(lambda s: (minutes_batch(s[0], min_time, step_time), divide_val(s[1], step_lat), divide_val(s[2], step_lon), s[3])) \
    .map(lambda s: (s[0], s[1], int(math.ceil(s[1])), s[2], int(math.ceil(s[2])), s[3]))

# time, lat, lon, xi, id
structured_weighted_data = structured_data\
    .map(lambda s: (s[0], s[2], s[4], get_weight((s[0], s[1], s[3])), s[5])) \

# number of points in 3D cells
keyValue_data = structured_weighted_data\
    .map(lambda s: (str(s[1]) + '_' + str(s[2]) + '_' + str(s[0]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda x: x[1] > 1) \

# calculate xi foreach cell
keyValue_weighted_data = structured_weighted_data \
    .map(lambda s: (str(s[1]) + '_' + str(s[2]) + '_' + str(s[0]), s[3])) \
    .reduceByKey(lambda a, b: a + b) \

# calculate the sum of xi
sum_x = keyValue_weighted_data.map(lambda s: s[1]).sum()

# calculate X
X = sum_x / number_of_cells

# calculate the sum of xi^2
sum_x2 = keyValue_weighted_data.map(lambda s: math.pow(s[1], 2)).sum()

# calculate S
S = math.sqrt((sum_x2 / number_of_cells) - math.pow(X, 2))

print '------------------' + str(X)

print_formatted(keyValue_data, 10)

print_formatted(keyValue_weighted_data, 5, key=lambda x: x[1])
