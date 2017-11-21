from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from sklearn import preprocessing
from pandas import *

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


sc = get_spark_context()
a = np.zeros((5, 5, 5))

print a

intiSource = sc.textFile(csv_file_path)\
    .map(lambda line: line.split(","))

print intiSource.count()

# dfa = pandas.DataFrame(a)

# print dfa

# testing()
# df.createOrReplaceTempView("points")

# df.groupBy("id").count().show()
# sqlDF = spark_session.sql("SELECT * FROM points WHERE id = 54265")
# sqlDF.show()

