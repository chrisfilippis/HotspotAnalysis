from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from sklearn import preprocessing


class SparkWrapper(object):

    app_name = "Hot spot app"
    master = "local"
    csv_file_path = "C:\Users\cfilip09\Desktop\d\\randomdata_small.csv"

    @staticmethod
    def get_spark_config():
        return SparkConf()\
            .setAppName(SparkWrapper.app_name)\
            .setMaster(SparkWrapper.master)

    def load_data(self, path="C:\Users\cfilip09\Desktop\d\\randomdata.csv", spark_session=None, file_format="csv", delimiter=","):
        if spark_session is None:
            spark_session = self.get_spark_session()

        data_frame = spark_session.read.format(file_format) \
            .option("delimiter", delimiter) \
            .option("header", "true") \
            .load(path)
        return data_frame

    @staticmethod
    def get_spark_session():
        return SparkSession \
            .builder \
            .appName(app_name) \
            .getOrCreate()

    def get_spark_context(self):
        configuration = self.get_spark_config()
        return SparkContext.getOrCreate(conf=configuration)

    @staticmethod
    def cast_column_to_type(data_frame, dt_col, col_type):
        return data_frame\
            .withColumn("new_id", data_frame[dt_col].cast(col_type,))\
            .drop(dt_col).withColumnRenamed("new_id", dt_col)

    def transform_schema(self, data_frame):
        data_frame = self.cast_column_to_type(data_frame, "id", "int")
        data_frame = self.cast_column_to_type(data_frame, "lat", "double")
        data_frame = self.cast_column_to_type(data_frame, "lon", "double")
        return data_frame

    @staticmethod
    def normalize_data(data_frame):
        pandas_data_frame = data_frame.toPandas()
        pandas_data_frame[['lat', 'lon']] = preprocessing.MinMaxScaler().fit_transform(pandas_data_frame[['lat', 'lon']])
        return pandas_data_frame
