from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from sklearn import preprocessing
from sklearn.cluster import KMeans
from pandas import *
import math

app_name = "Hot spot app"
master = "local"
csv_file_path = "C:\Users\cfilip09\Desktop\d\\randomdata_small.csv"


def get_spark_config():
    return SparkConf()\
        .setAppName(app_name)\
        .setMaster(master)


def load_data(path="C:\Users\cfilip09\Desktop\d\\randomdata.csv", spark_session=None, file_format="csv", delimiter=","):
    if spark_session is None:
        spark_session = get_spark_session()

    data_frame = spark_session.read.format(file_format) \
        .option("delimiter", delimiter) \
        .option("header", "true") \
        .load(path)
    return data_frame


def get_spark_session():
    return SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()


def get_spark_context():
    configuration = get_spark_config()
    return SparkContext.getOrCreate(conf=configuration)


def transform_column(data_frame, dt_col, col_type):
    return data_frame\
        .withColumn("new_id", data_frame[dt_col].cast(col_type,))\
        .drop(dt_col).withColumnRenamed("new_id", dt_col)


def transform_schema(data_frame):
    data_frame = transform_column(data_frame, "id", "int")
    data_frame = transform_column(data_frame, "lat", "double")
    data_frame = transform_column(data_frame, "lon", "double")
    return data_frame


def normalize_data(data_frame):
    pandas_data_frame = data_frame.toPandas()
    pandas_data_frame[['lat', 'lon']] = preprocessing.MinMaxScaler().fit_transform(pandas_data_frame[['lat', 'lon']])
    return pandas_data_frame


def testing():

    sc = get_spark_context()
    spk_session = get_spark_session()
    sql_context = SQLContext(sc)

    print "load data"

    df = load_data(csv_file_path, spk_session, "csv")
    df = transform_schema(df)

    print df.show()

    pandas_df = normalize_data(df)
    df = sql_context.createDataFrame(pandas_df)

    print df.take(20)


def find_ceil_value(lat, step):
    return math.ceil(lat/step)


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
spark = get_spark_session()

step_lat = 0.01
step_lon = 0.01

initSource = spark.read\
    .option("header", "true")\
    .csv(csv_file_path)

rdd = initSource.rdd \
    .map(lambda s: (int(s[0]), find_ceil_value(float(s[1]), step_lat), find_ceil_value(float(s[2]), step_lon), int(s[3])))

print "csv"
print rdd.collect()
