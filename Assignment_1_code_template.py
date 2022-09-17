from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


# Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True

    except:
        return False


# Function - Cleaning
# For example, remove lines if they don’t have 16 values and
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles,
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if (len(p) == 17):
        if (isfloat(p[5]) and isfloat(p[11])):
            if (float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0):
                return p


# Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Assignment-1")
    m1 = sc.parallelize([1, 3, 2, 3, 45, 5])
    m1.take(5)


    rdd = sc.textFile(sys.argv[1])

    # clean the data
    rdd1 = rdd.map(lambda x: x.split(","))
    clean_rdd = rdd1.filter(correctRows)

    # Task 1
    # Your code goes here

    taxi_driver_rdd = clean_rdd.map(lambda x: (x[0], x[1])).distinct()

    aggregate_rdd = taxi_driver_rdd.aggregateByKey(0, lambda x, y: x + 1, lambda x, y: x + y)
    results_1 = aggregate_rdd.map(lambda x: (x[1], x[0]))
    results_top = results_1.top(10)
    print(results_top)
    results_1.coalesce(1).sortByKey(False).saveAsTextFile(sys.argv[2])


    # Task 2
    # Your code goes here
    def list_add(x, y):
        x.add(y)
        return x
    def list_update(x, y):
        x.update(y)
        return x

    driver_earn_rdd = clean_rdd.map(lambda x: (x[1], float(x[16]) / float(x[4]) * 60))
    aggregate_rdd = driver_earn_rdd.aggregateByKey(set(), list_add, list_update)
    aggregate_len_rdd = aggregate_rdd.mapValues(lambda x: len(x))
    aggregate_earn_rdd = driver_earn_rdd.reduceByKey(lambda x, y: x + y)
    avg_earn_rdd = aggregate_earn_rdd.join(aggregate_len_rdd)
    avg_earn_rdd.filter(lambda x: x[1][0] > 0 and x[1][1] > 0)
    results_2 = avg_earn_rdd.map(lambda x: (x[1][0] / x[1][1], x[0]))
    results_top = results_2.top(10)
    print(results_top)

    # savings output to argument
    results_2.coalesce(1).sortByKey(False).saveAsTextFile(sys.argv[3])

    # Task 3 - Optional
    # Your code goes here

    # Task 4 - Optional
    # Your code goes here

    sc.stop()
