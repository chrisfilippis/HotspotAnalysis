from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from scipy.spatial import distance
import math as math
import pysal.esda
from pyspark.sql import SQLContext
from sklearn import preprocessing

app_name = "Hot spot app"
master = "local[*]"


def minutes_batch(unix_timestamp, min_t, step):
    step_seconds = 60 * step
    difference = unix_timestamp - min_t;
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
        .setMaster(master) \
        .set("spark.executor.cores", "4")


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


def final_transform(s, min_tim):
    f_trans = (minutes_batch(s[0], min_tim.value, step_time), divide_val(s[1], step_lat), divide_val(s[2], step_lon), s[3])
    s_trans = (f_trans[0], f_trans[1], int(math.ceil(f_trans[1])), f_trans[2], int(math.ceil(f_trans[2])), f_trans[3])
    t_trans = (s_trans[0], s_trans[2], s_trans[4], get_weight((s_trans[0], s_trans[1], s_trans[3])), s_trans[5])
    return t_trans

# RDD.persists()
# dic = wrapper.get_spark_context().parallelize(rdd)
# collect() == collect all rdd data in a computers memory
# a = np.zeros((5, 5, 5))


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
csv_file_path = "C:\Spark_Data\data.sample"
# csv_file_path = "C:\Spark_Data\million_bigdata.sample"
acc_number_of_cells = sc.accumulator(0)
acc_sum_x = sc.accumulator(0)
acc_sum_x2 = sc.accumulator(0)


initSource = sc.textFile(csv_file_path)\
    .map(lambda line: line.split(" "))

# string: time, string: lat, string: lon, string: id
source = initSource \
    .map(lambda s: (int(s[0]), float(s[2]), float(s[3]), int(s[1])))

# find the minimum date
min_time = source \
    .map(lambda s: s[0]) \
    .min()

broadcast_min_time = sc.broadcast(min_time)
print '>>>>>>>>>>>>>>>>>>' + str(broadcast_min_time.value)

# time, lat, lon, xi, id
structured_weighted_data = source \
    .map(lambda ss: final_transform(ss, broadcast_min_time)) \
    .persist(StorageLevel.MEMORY_AND_DISK)

# number of points in 3D cells
keyValue_data = structured_weighted_data\
    .map(lambda s: (str(s[1]) + '_' + str(s[2]) + '_' + str(s[0]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda x: x[1] > 1) \

# calculate xi foreach cell
keyValue_weighted_data = structured_weighted_data \
    .map(lambda s: (str(s[1]) + '_' + str(s[2]) + '_' + str(s[0]), s[3])) \
    .reduceByKey(lambda a, b: a + b) \


################
# Accumulators #
################

# calculate the sum of xi using accumulator sum_x
keyValue_weighted_data.foreach(lambda s: acc_sum_x.add(s[1]))
# find the number of cells using accumulator number_of_cells
initSource.foreach(lambda s: acc_number_of_cells.add(1))
# calculate the sum of xi^2 using accumulator sum_x2
keyValue_weighted_data.foreach(lambda s: acc_sum_x2.add(math.pow(s[1], 2)))

# get values
number_of_cells = acc_number_of_cells.value
sum_x = acc_sum_x.value
sum_x2 = acc_sum_x2.value

# calculate X
X = sum_x / number_of_cells

# calculate S
S = math.sqrt((sum_x2 / number_of_cells) - math.pow(X, 2))

print '########################'
print '#### number_of_cells = ' + str(number_of_cells)
print '#### sum_x = ' + str(sum_x)
print '#### sum_x2 = ' + str(sum_x2)
print '#### X = ' + str(X)
print '#### S = ' + str(S)
print '########################'

print_formatted(keyValue_data, 10)

print_formatted(keyValue_weighted_data, 5, key=lambda x: x[1])
