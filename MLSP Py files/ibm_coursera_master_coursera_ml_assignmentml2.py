# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ml_AssignmentML2.ipynb


This is the second assignment for the Coursera course "Advanced Machine Learning and Signal Processing"


Just execute all cells one after the other and you are done - just note that in the last one you have to update your email address (the one you've used for coursera) and obtain a submission token, you get this from the programming assignment directly on coursera.

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

"""Please create a VectorAssembler which consumes columns X, Y and Z and produces a column “features”

"""

from pyspark.ml.feature import VectorAssembler
vectorAssembler = ###YOUR_CODE_GOES_HERE###"

"""Please instantiate a classifier from the SparkML package and assign it to the classifier variable. Make sure to either
1.	Rename the “CLASS” column to “label” or
2.	Specify the label-column correctly to be “CLASS”

"""

from pyspark.ml.classification import ###YOUR_CODE_GOES_HERE###"

classifier = ###YOUR_CODE_GOES_HERE###"

"""Let’s train and evaluate…

"""

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vectorAssembler, classifier])

model = pipeline.fit(df)

prediction = model.transform(df)

prediction.show()

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
binEval = MulticlassClassificationEvaluator().setMetricName("accuracy") .setPredictionCol("prediction").setLabelCol("CLASS")

binEval.evaluate(prediction)

"""If you are happy with the result (I’m happy with > 0.55) please submit your solution to the grader by executing the following cells, please don’t forget to obtain an assignment submission token (secret) from the Coursera’s graders web page and paste it to the “secret” variable below, including your email address you’ve used for Coursera. (0.55 means that you are performing better than random guesses)

"""

!rm -Rf a2_m2.json

prediction = prediction.repartition(1)
prediction.write.json('a2_m2.json')

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

import zipfile
import os

def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

zipf = zipfile.ZipFile('a2_m2.json.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('a2_m2.json', zipf)
zipf.close()

!base64 a2_m2.json.zip > a2_m2.json.zip.base64

from rklib import submit
key = "J3sDL2J8EeiaXhILFWw2-g"
part = "G4P6f"
email = None###YOUR_CODE_GOES_HERE###"
token = None###YOUR_CODE_GOES_HERE###" # (have a look here if you need more information on how to obtain the token https://youtu.be/GcDo0Rwe06U?t=276)

with open('a2_m2.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, token, key, part, [part], data)
