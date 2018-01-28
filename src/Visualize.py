from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import gmplot

app_name = "Hot spot app"
master = "local[*]"


def get_spark_config():
    return SparkConf()\
        .setAppName(app_name)\
        .setMaster(master) \
        .set("spark.executor.cores", "4")\
        .set("spark.driver.memory", "4g")


def get_spark_context():
    configuration = get_spark_config()
    return SparkContext.getOrCreate(conf=configuration)


def print_formatted(points_to_print, top=20, key=None):

    if key is None:
        for pri in points_to_print.top(top):
            print pri
    else:
        for pri in points_to_print.top(top, key=key):
            print pri


sc = get_spark_context()
sqlContext = SQLContext(sc)
csv_file_path = "C:\Spark_Data\output\data_out.csv"


initSource = sc.textFile(csv_file_path)\
    .map(lambda x: x.split(",")[0].split("_"))\

# int: time, float: lat, float: lon, int: id
visual_data = initSource \
    .map(lambda x: ((str(int(x[0])) + '_' + str(int(x[1]))), 1)) \
    .reduceByKey(lambda x, y: x + y) \

sorted_visual_source = visual_data.takeOrdered(1000, key=lambda x: -x[1])

print sorted_visual_source

gmap = gmplot.GoogleMapPlotter.from_geocode("San Francisco")
gmap.draw("mymap.html")