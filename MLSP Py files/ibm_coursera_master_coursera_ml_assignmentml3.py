# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ml_AssignmentML3.ipynb



This is the third assignment for the Coursera course "Advanced Machine Learning and Signal Processing"

Just execute all cells one after the other and you are done - just note that in the last one you must update your email address (the one you've used for coursera) and obtain a submission token, you get this from the programming assignment directly on coursera.

Please fill in the sections labelled with "###YOUR_CODE_GOES_HERE###"
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

"""Now it’s time to have a look at the recorded sensor data. You should see data similar to the one exemplified below….

"""

df=spark.read.load('a2.parquet')

df.createOrReplaceTempView("df")
spark.sql("SELECT * from df").show()

"""Let’s check if we have balanced classes – this means that we have roughly the same number of examples for each class we want to predict. This is important for classification but also helpful for clustering"""

spark.sql("SELECT count(class), class from df group by class").show()

"""Let's create a VectorAssembler which consumes columns X, Y and Z and produces a column “features”

"""

from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols=["X","Y","Z"],
                                  outputCol="features")

"""Please insatiate a clustering algorithm from the SparkML package and assign it to the clust variable. Here we don’t need to take care of the “CLASS” column since we are in unsupervised learning mode – so let’s pretend to not even have the “CLASS” column for now – but it will become very handy later in assessing the clustering performance. PLEASE NOTE – IN REAL-WORLD SCENARIOS THERE IS NO CLASS COLUMN – THEREFORE YOU CAN’T ASSESS CLASSIFICATION PERFORMANCE USING THIS COLUMN


"""

from pyspark.ml.clustering ###YOUR_CODE_GOES_HERE###

clust = ###YOUR_CODE_GOES_HERE###

"""Let’s train...

"""

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vectorAssembler, clust])
model = pipeline.fit(df)

"""...and evaluate..."""

prediction = model.transform(df)
prediction.show()

prediction.createOrReplaceTempView('prediction')
spark.sql('''
select max(correct)/max(total) as accuracy from (

    select sum(correct) as correct, count(correct) as total from (
        select case when class != prediction then 1 else 0 end as correct from prediction
    )

    union

    select sum(correct) as correct, count(correct) as total from (
        select case when class = prediction then 1 else 0 end as correct from prediction
    )
)
''').rdd.map(lambda row: row.accuracy).collect()[0]

"""If you reached at least 55% of accuracy you are fine to submit your predictions to the grader. Otherwise please experiment with parameters setting to your clustering algorithm, use a different algorithm or just re-record your data and try to obtain. In case you are stuck, please use the Coursera Discussion Forum. Please note again – in a real-world scenario there is no way in doing this – since there is no class label in your data. Please have a look at this further reading on clustering performance evaluation https://en.wikipedia.org/wiki/Cluster_analysis#Evaluation_and_assessment

"""

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

!rm -Rf a2_m3.json

prediction= prediction.repartition(1)
prediction.write.json('a2_m3.json')

import zipfile
import os

def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

zipf = zipfile.ZipFile('a2_m3.json.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('a2_m3.json', zipf)
zipf.close()

!base64 a2_m3.json.zip > a2_m3.json.zip.base64

from rklib import submit
key = "pPfm62VXEeiJOBL0dhxPkA"
part = "EOTMs"
email = None###YOUR_CODE_GOES_HERE###
token = None###YOUR_CODE_GOES_HERE### # (have a look here if you need more information on how to obtain the token https://youtu.be/GcDo0Rwe06U?t=276)


with open('a2_m3.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, token, key, part, [part], data)
