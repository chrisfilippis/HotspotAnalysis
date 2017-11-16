from pyspark import SparkConf, SparkContext

import findspark
findspark.init()

# Or the following command
findspark.init("/path/to/spark_home")

# sc = SparkContext.getOrCreate()

# conf = SparkConf()
# conf.setMaster("local")
# conf.setAppName("Hot spot app")
# conf.set("spark.executor.memory", "1g")
# sc = SparkContext(conf=conf)

# print sc.toDebugString()