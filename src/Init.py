from pyspark import SparkConf, SparkContext

appName = "Hot spot app"
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

print "sssssssssssssssssssstart"

intiSource = sc.textFile("C:\Users\cfilip09\Desktop\d\data1.csv")\
    .map(lambda line: line.split(","))

filteredSource1 = intiSource\
    .filter(lambda line: line[0] == "1") \
    .map(lambda line: (line[0], line[1])) \

filteredSource3 = intiSource\
    .filter(lambda line: line[0] == "3") \
    .map(lambda line: (line[0], line[1])) \

filteredSource5 = intiSource\
    .filter(lambda line: line[0] == "5") \
    .map(lambda line: (line[0], line[1])) \

filteredSource9 = intiSource\
    .filter(lambda line: line[0] == "9") \
    .map(lambda line: (line[0], line[1])) \


print "count1--" + str(filteredSource1.count())
print "count3--" + str(filteredSource3.count())
print "count5--" + str(filteredSource5.count())
print "count9--" + str(filteredSource9.count())

print intiSource
