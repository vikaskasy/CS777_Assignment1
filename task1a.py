from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Toptaxis <file> <output> ", file=sys.stderr)
        exit(-1)
    ##Initialize Spark Context
    sc = SparkContext.getOrCreate()
    print(sc.version)
    #Read the text file
    rdd = sc.textFile(sys.argv[1])
    spark = SparkSession.builder.appName('Task1').getOrCreate()

    #function to clean up data, checking columns if the datatype is appropriate or not (taken from helper code)
    def correctRows(p):
        if(len(p)==17):
            if(float(p[5]) and float(p[11])):
                if(float(p[4])> 60 and float(p[5])>0.10 and float(p[11])> 0.10 and float(p[16])> 0.10):
                    return p
    #reading using sparksession
    df = spark.read.csv(sys.argv[1])
    #mapping to a tuple
    rdd1 = df.rdd.map(tuple)
    #calling the function to clean data
    taxiclean = rdd1.filter(correctRows)
    # take distinct rows for the combination of medallion and drivers , map the medallions,1 and reduce by key and take top 10
    t = taxiclean.map(lambda x:((x[0],x[1]))).distinct()\
        .map(lambda x:(x[0],1))\
        .reduceByKey(lambda a,b: a+b)\
        .top(10, lambda x:x[1])

    print(t)
    #coalesce the output and providing in a file
    topTaxi = sc.parallelize(t).coalesce(1)
    topTaxi.saveAsTextFile(sys.argv[2])

    #stopping the cluster
    sc.stop()
    spark.stop()