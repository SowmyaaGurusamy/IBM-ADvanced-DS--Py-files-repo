# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ml_AssignmentML4.ipynb



# PLEASE NOTE: Please run this notebook OUTSIDE a Spark notebook as it should run in a plain Python 3.9 XS Environment (2 vCPU) Environment

This is the last assignment for the Coursera course "Advanced Machine Learning and Signal Processing"

Just execute all cells one after the other and you are done - just note that in the last one you should update your email address (the one you've used for coursera) and obtain a submission token, you get this from the programming assignment directly on coursera.

Please fill in the sections labelled with "###YOUR_CODE_GOES_HERE###"

The purpose of this assignment is to learn how feature engineering boosts model performance. You will apply Discrete Fourier Transformation on the accelerometer sensor time series and therefore transforming the dataset from the time to the frequency domain.

After that, you’ll use a classification algorithm of your choice to create a model and submit the new predictions to the grader. Done.
"""

!pip install pyspark==3.2.1 systemds==2.2.1

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .getOrCreate()

"""
So the first thing we need to ensure is that we are on the latest version of SystemML, which is 1.3.0 (as of 20th March'19) Please use the code block below to check if you are already on 1.3.0 or higher. 1.3 contains a necessary fix, that's we are running against the SNAPSHOT
"""

!wget https://github.com/IBM/coursera/blob/master/coursera_ml/shake.parquet?raw=true
!mv shake.parquet?raw=true shake.parquet

"""Now it’s time to read the sensor data and create a temporary query table."""

df=spark.read.parquet('shake.parquet')

df.show()

df.createOrReplaceTempView("df")

"""We’ll use Apache SystemML to implement Discrete Fourier Transformation. This way all computation continues to happen on the Apache Spark cluster for advanced scalability and performance.

As you’ve learned from the lecture, implementing Discrete Fourier Transformation in a linear algebra programming language is simple. Apache SystemML DML is such a language and as you can see the implementation is straightforward and doesn’t differ too much from the mathematical definition (Just note that the sum operator has been swapped with a vector dot product using the %*% syntax borrowed from R
):

<img style="float: left;" src="https://wikimedia.org/api/rest_v1/media/math/render/svg/1af0a78dc50bbf118ab6bd4c4dcc3c4ff8502223">
"""

from pyspark.sql.functions import monotonically_increasing_id
from systemds.context import SystemDSContext
import numpy as np
import pandas as pd

def dft_systemds(signal,name):


    with SystemDSContext(spark) as sds:
        size = signal.count()
        signal = sds.from_numpy(signal.toPandas().to_numpy())
        pi = sds.scalar(3.141592654)

        n = sds.seq(0,size-1)
        k = sds.seq(0,size-1)

        M = (n @ (k.t())) * (2*pi/size)

        Xa = M.cos() @ signal
        Xb = M.sin() @ signal

        index = (list(map(lambda x: [x],np.array(range(0, size, 1)))))
        DFT = np.hstack((index,Xa.cbind(Xb).compute()))
        DFT_pdf = pd.DataFrame(DFT, columns=list(["id",name+'_sin',name+'_cos']))
        DFT_df = spark.createDataFrame(DFT_pdf)
        return DFT_df

"""Now it’s time to create a function which takes a single row Apache Spark data frame as argument (the one containing the accelerometer measurement time series for one axis) and returns the Fourier transformation of it. In addition, we are adding an index column for later joining all axis together and renaming the columns to appropriate names. The result of this function is an Apache Spark DataFrame containing the Fourier Transformation of its input in two columns.

Now it’s time to create individual DataFrames containing only a subset of the data. We filter simultaneously for accelerometer each sensor axis and one for each class. This means you’ll get 6 DataFrames. Please implement this using the relational API of DataFrames or SparkSQL. Please use class 1 and 2 and not 0 and 1. <h1><span style="color:red">Please make sure that each DataFrame has only ONE colum (only the measurement, eg. not CLASS column)</span></h1>
"""

x0 = ###YOUR_CODE_GOES_HERE### => Please create a DataFrame containing only measurements of class 0 from the x axis
y0 = ###YOUR_CODE_GOES_HERE### => Please create a DataFrame containing only measurements of class 0 from the y axis
z0 = ###YOUR_CODE_GOES_HERE### => Please create a DataFrame containing only measurements of class 0 from the z axis
x1 = ###YOUR_CODE_GOES_HERE### => Please create a DataFrame containing only measurements of class 1 from the x axis
y1 = ###YOUR_CODE_GOES_HERE### => Please create a DataFrame containing only measurements of class 1 from the y axis
z1 = ###YOUR_CODE_GOES_HERE### => Please create a DataFrame containing only measurements of class 1 from the z axis

"""Since we’ve created this cool DFT function before, we can just call it for each of the 6 DataFrames now. And since the result of this function call is a DataFrame again we can use the pyspark best practice in simply calling methods on it sequentially. So what we are doing is the following:

- Calling DFT for each class and accelerometer sensor axis.
- Joining them together on the ID column.
- Re-adding a column containing the class index.
- Stacking both Dataframes for each classes together


"""

from pyspark.sql.functions import lit

df_class_0 = dft_systemds(x0,'x') \
    .join(dft_systemds(y0,'y'), on=['id'], how='inner') \
    .join(dft_systemds(z0,'z'), on=['id'], how='inner') \
    .withColumn('class', lit(0))

df_class_1 = dft_systemds(x1,'x') \
    .join(dft_systemds(y1,'y'), on=['id'], how='inner') \
    .join(dft_systemds(z1,'z'), on=['id'], how='inner') \
    .withColumn('class', lit(1))

df_dft = df_class_0.union(df_class_1)

df_dft.show()

"""Please create a VectorAssembler which consumes the newly created DFT columns and produces a column “features”

"""

from pyspark.ml.feature import VectorAssembler

vectorAssembler = ###YOUR_CODE_GOES_HERE###

"""Please insatiate a classifier from the SparkML package and assign it to the classifier variable. Make sure to set the “class” column as target.

"""

from pyspark.ml.classification import GBTClassifier

classifier = ###YOUR_CODE_GOES_HERE###

"""Let’s train and evaluate…

"""

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vectorAssembler, classifier])

model = pipeline.fit(df_dft)

prediction = model.transform(df_dft)

prediction.show()

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
binEval = MulticlassClassificationEvaluator().setMetricName("accuracy") .setPredictionCol("prediction").setLabelCol("class")

binEval.evaluate(prediction)

"""If you are happy with the result (I’m happy with > 0.8) please submit your solution to the grader by executing the following cells, please don’t forget to obtain an assignment submission token (secret) from the Courera’s graders web page and paste it to the “secret” variable below, including your email address you’ve used for Coursera.

"""

!rm -Rf a2_m4.json

prediction = prediction.repartition(1)
prediction.write.json('a2_m4.json')

!rm -f rklib.py
!wget wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

from rklib import zipit
zipit('a2_m4.json.zip','a2_m4.json')

!base64 a2_m4.json.zip > a2_m4.json.zip.base64

from rklib import submit
key = "-fBiYHYDEeiR4QqiFhAvkA"
part = "IjtJk"
email = ###YOUR_CODE_GOES_HERE###
submission_token = ###YOUR_CODE_GOES_HERE### # (have a look here if you need more information on how to obtain the token https://youtu.be/GcDo0Rwe06U?t=276)

with open('a2_m4.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, submission_token, key, part, [part], data)
