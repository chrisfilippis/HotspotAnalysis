from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


appName = "Hot spot app"
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master)

appName = "Hot spot app"
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sqlContext = SQLContext(sc)

print "sssssssssssssssssssstart"

df = spark.read.format("csv") \
    .option("delimiter", ",") \
    .option("header", "true") \
    .load("C:\Users\cfilip09\Desktop\d\\randomdata.csv")

df.createDataFrame("people")

df.groupBy("id").count().show()
sqlDF = spark.sql("SELECT * FROM people WHERE id = 54265")
sqlDF.show()

# Displays the content of the DataFrame to stdout
# df.show()
