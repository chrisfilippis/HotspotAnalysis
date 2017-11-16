from pyspark import SparkConf, SparkContext

appName = "Hot spot app"
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

print "Rrrrrrready"

text_file = sc.textFile("C:\Users\cfilip09\Desktop\d\loremipsum.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
# counts.saveAsTextFile("C:\Users\cfilip09\Desktop\d\wordcount.txt")

print counts.collect()
