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


def transform_schema(data_frame):
    return data_frame.withColumn("new_id", data_frame.id.cast("int",)).drop("id").withColumnRenamed("new_id", "id")


def normalize_data(data_frame, input_col, output_col):
    x = data_frame.toPandas()    # returns a numpy array
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    data_frame = pandas.DataFrame(x_scaled)
    return data_frame

print "init"

sc = get_spark_context()
spk_session = get_spark_session()
sqlContext = SQLContext(sc)

print "load data"

df = load_data(csv_file_path, spk_session, "csv")
df = transform_schema(df)

print df.show()

df = spk_session.createDataFrame(normalize_data(df, "id", "norm_id"))

print df.show()

# print newDf

print "temp View"

# df.createOrReplaceTempView("points")

# df.groupBy("id").count().show()
# sqlDF = spark_session.sql("SELECT * FROM points WHERE id = 54265")
# sqlDF.show()

