from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from scipy.spatial import distance
import math as math
from pyspark.sql import SQLContext
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


def transform_with_weight(s, min_tim, time_step):
    f_trans = (minutes_batch(s[0], min_tim.value, time_step), divide_val(s[1], step_lat), divide_val(s[2], step_lon), s[3])
    s_trans = (f_trans[0], f_trans[1], int(math.ceil(f_trans[1])), f_trans[2], int(math.ceil(f_trans[2])), f_trans[3])
    t_trans = (s_trans[0], s_trans[2], s_trans[4], get_weight((s_trans[0], s_trans[1], s_trans[3])), s_trans[5])
    return t_trans


def get_key(line, part1=1, part2=2, part3=0):
    return str(line[part1]) + '_' + str(line[part2]) + '_' + str(line[part3])


def handle_accumulators(x, _sum_x, _sum_x2):
    _sum_x.add(x[1])
    _sum_x2.add(math.pow(x[1], 2))


def get_min_max(init_rdd, index):
    return init_rdd.map(lambda x: x[index]).min(), init_rdd.map(lambda x: x[index]).max()


def get_direct_neighbor_ids(cell, t_min, t_max, ln_min, ln_max, lt_min, lt_max, cell_xi):
    key_parts = cell.split("_")
    lat, lon, time = int(key_parts[0]), int(key_parts[1]), int(key_parts[2])
    result_tuples = []

    lat_from = lat if lt_min == lat else lat - 1
    lat_to = lat if lt_max == lat else lat + 1

    lon_from = lon if ln_min == lon else lon - 1
    lon_to = lon if ln_max == lon else lon + 1

    time_from = time if t_min == time else time - 1
    time_to = time if t_max == time else time + 1

    for x in xrange(lat_from, lat_to + 1):
        for y in xrange(lon_from, lon_to + 1):
            for z in xrange(time_from, time_to + 1):
                if not (lat == x and lon == y and time == z):
                    result_tuples.append((str(x) + "_" + str(y) + "_" + str(z), cell_xi))

    return result_tuples


def get_getisord(cell, sumxi, n, large_x, large_s, t_min, t_max, ln_min, ln_max, lt_min, lt_max):

    nci = len(get_direct_neighbor_ids(cell, t_min, t_max, ln_min, ln_max, lt_min, lt_max, 0))

    sqrt_val = ((n * nci) - math.pow(nci, 2)) / (n - 1)
    gi = (sumxi - (large_x * nci)) / (large_s * math.sqrt(sqrt_val))

    return cell, gi


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
sqlContext = SQLContext(sc)
step_lat = 0.01
step_lon = 0.01
step_time = 120
csv_file_path = "C:\Spark_Data\data.sample"
# csv_file_path = "C:\Spark_Data\million_bigdata.sample"

acc_number_of_cells = sc.accumulator(0)
acc_sum_x = sc.accumulator(0)
acc_sum_x2 = sc.accumulator(0)

initSource = sc.textFile(csv_file_path)\
    .map(lambda x: x.split(" "))

# int: time, float: lat, float: lon, int: id
source = initSource \
    .map(lambda x: (int(x[0]), float(x[2]), float(x[3]), int(x[1])))

# find the min date
broad_time_min = source\
     .map(lambda x: x[0]).min()

broadcast_min_time = sc.broadcast(broad_time_min)

# time, lat, lon, xi, id
structured_weighted_data = source \
    .map(lambda x: transform_with_weight(x, broadcast_min_time, step_time)) \
    .persist(StorageLevel.MEMORY_AND_DISK)

# find the min / max longitude
lon_min, lon_max = get_min_max(structured_weighted_data, 2)
lon_range = lon_max - lon_min

# find the min / max latitude
lat_min, lat_max = get_min_max(structured_weighted_data, 1)
lat_range = lat_max - lat_min

# find the min / max date
time_min, time_max = get_min_max(structured_weighted_data, 0)
time_range = time_max - time_min

n = lat_range * lat_range * time_range

# number of points in 3D cells
keyValue_data = structured_weighted_data\
    .map(lambda x: (get_key(x), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .filter(lambda x: x[1] > 1) \

print str(keyValue_data.count())

# calculate xi foreach cell
keyValue_weighted_data = structured_weighted_data \
    .map(lambda x: (get_key(x), x[3])) \
    .reduceByKey(lambda x, y: x + y) \

# calculate the sum of xi and xi^2 using accumulator sum_x and acc_sum_x2
keyValue_weighted_data.foreach(lambda x: handle_accumulators(x, acc_sum_x, acc_sum_x2))

# find the number of cells using accumulator number_of_cells
initSource.foreach(lambda x: acc_number_of_cells.add(1))

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
print '#### sum_x           = ' + str(sum_x)
print '#### sum_x2          = ' + str(sum_x2)
print '#### X               = ' + str(X)
print '#### S               = ' + str(S)
print '#### n               = ' + str(n)
print '#### lon range       = ' + str(lon_min) + " / " + str(lon_max)
print '#### lat range       = ' + str(lat_min) + " / " + str(lat_max)
print '#### time range      = ' + str(time_min) + " / " + str(time_max)
print '########################'

keyValue_with_neighbor_weights = keyValue_weighted_data\
    .flatMap(lambda line: get_direct_neighbor_ids(line[0], time_min, time_max, lon_min, lon_max, lat_min, lat_max, line[1])) \
    .reduceByKey(lambda x, y: x + y)

# cell, cell_xi, n, large_x, large_s, t_min, t_max, ln_min, ln_max, lt_min, lt_max, cell_xi
getis_ord_keyValue = keyValue_with_neighbor_weights\
    .map(lambda line: get_getisord(line[0], line[1], n, X, S, time_min, time_max, lon_min, lon_max, lat_min, lat_max))


weight_dataFrame = sqlContext.createDataFrame(keyValue_with_neighbor_weights, ['id', 'sumxi'])
getis_dataFrame = sqlContext.createDataFrame(getis_ord_keyValue, ['id', 'gi'])

weight_dataFrame.limit(20).show()
getis_dataFrame.sort(['gi'], ascending=[0]).limit(20).show()
# getis_dataFrame.sort(['gi'], ascending=[0]).limit(20).coalesce(1).rdd.saveAsTextFile("C:\Spark_Data\output")
# print_formatted(keyValue_with_neighbor_weights, 20)
# print_formatted(keyValue_data, 10)
