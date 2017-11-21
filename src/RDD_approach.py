from SparkWrapper import *

wrapper = SparkWrapper()
sc = wrapper.get_spark_context()

print wrapper.csv_file_path
dd = sc.textFile(wrapper.csv_file_path).map(lambda s: len(s))

print dd.take(1)
