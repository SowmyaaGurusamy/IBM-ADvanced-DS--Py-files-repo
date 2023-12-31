# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ml_AssignmentML1.ipynb


This is the first assignment for the Coursera course "Advanced Machine Learning and Signal Processing"

Just execute all cells one after the other and you are done - just note that in the last one you have to update your email address (the one you've used for coursera) and obtain a submission token, you get this from the programming assignment directly on coursera.
"""

!pip install pyspark==2.4.5

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('<<<<<!!!!! Please restart your kernel after installing Apache Spark !!!!!>>>>>')

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

!wget https://github.com/IBM/coursera/raw/master/coursera_ml/a2.parquet

df=spark.read.load('a2.parquet')

df.createOrReplaceTempView("df")
spark.sql("SELECT * from df").show()

!rm -Rf a2_m1.json

df = df.repartition(1)
df.write.json('a2_m1.json')

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

import zipfile
import os

def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

zipf = zipfile.ZipFile('a2_m1.json.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('a2_m1.json', zipf)
zipf.close()

!base64 a2_m1.json.zip > a2_m1.json.zip.base64

from rklib import submit
key = "1injH2F0EeiLlRJ3eJKoXA"
part = "wNLDt"
email = "###YOUR_CODE_GOES_HERE###"
token = "###YOUR_CODE_GOES_HERE###" #(have a look here if you need more information on how to obtain the token https://youtu.be/GcDo0Rwe06U?t=276)

with open('a2_m1.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, token, key, part, [part], data)

