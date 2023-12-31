# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ds_assignment1.2.ipynb


# Warmup Assignment 2
Below you see some ApacheSpark code written in Python. You don't have to change code now, the only thing we want you to do is to make sure that you have a proper Apache Spark Notebook environment available for this course
"""

!pip install pyspark==3.3.0

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('<<<<<!!!!! Please restart your kernel after installing Apache Spark !!!!!>>>>>')

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

def assignment1(sc):
    rdd = sc.parallelize(list(range(100)))
    return rdd.count()

print(assignment1(sc))

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

"""Please provide your email address and obtain a submission token on the grader’s submission page in coursera, then execute the cell"""

from rklib import submit
import json

key = "R1eDmiHNEei9kxIYdin0mA"
part = "fnFg7"
email = ###_YOUR_CODE_GOES_HERE_###
token = ###_YOUR_CODE_GOES_HERE_### #you can obtain it from the grader page on Coursera (have a look here if you need more information https://youtu.be/GcDo0Rwe06U?t=276)


submit(email, token, key, part, [part], json.dumps(assignment1(sc)))

