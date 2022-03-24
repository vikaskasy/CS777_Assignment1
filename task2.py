from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: drivers <file> <output> ", file=sys.stderr)
        exit(-1)
     
    ##Initialize Spark Context
    sc = SparkContext.getOrCreate()
    print(sc.version)
    rdd = sc.textFile(sys.argv[1])
    #create sparksession
    spark = SparkSession.builder.appName('Task1').getOrCreate()

    #function to clean up data, checking columns if the datatype is appropriate or not (taken from helper code)
    def correctRows(p):
        if(len(p)==17):
            if(float(p[5]) and float(p[11])):
                if(float(p[4])> 60 and float(p[5])>0.10 and float(p[11])> 0.10 and float(p[16])> 0.10):
                    return p
    #reading the file and mapping to tuple 
    df = spark.read.csv(sys.argv[1])
    rdd1 = df.rdd.map(tuple)
    #calling the function to clean data
    taxiclean = rdd1.filter(correctRows)
    #taking key as driver and trip duration and total money as key , then reduce by key and values , convert seconds to minutes and take top 10
    d = taxiclean.map(lambda x:((x[1]),(float((x[4])),float((x[16])))))\
    .reduceByKey(lambda a,b:(a[0]+b[0], a[1] + b[1]))\
    .map(lambda x: (x[0], x[1][1]/(x[1][0]/60)))\
    .top(10, lambda x:x[1])

    print(d)
    #
    topTaxi = sc.parallelize(d).coalesce(1)
    topTaxi.saveAsTextFile(sys.argv[2])
    
    #stopping the cluster and spark
    sc.stop()
    spark.stop()