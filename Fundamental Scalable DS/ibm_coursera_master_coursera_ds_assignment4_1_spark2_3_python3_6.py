# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ds_assignment4.1_spark2.3_python3.6.ipynb



# Assignment 4

Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.

Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
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

"""Sampling is one of the most important things when it comes to visualization because often the data set gets so huge that you simply

- can't copy all data to a local Spark driver (Watson Studio is using a "local" Spark driver)
- can't throw all data at the plotting library

Please implement a function which returns a 10% sample of a given data frame:
"""

def getSample():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    #https://spark.apache.org/docs/latest/api/sql/
    return df.#YOUR CODE GOES HERE(False,#YOUR CODE GOES HERE)

"""Now we want to create a histogram and boxplot. Please ignore the sampling for now and return a python list containing all temperature values from the data set"""

def getListForHistogramAndBoxPlot():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    #https://spark.apache.org/docs/latest/api/sql/
    my_list = spark.sql("""
        SELECT #YOUR CODE GOES HERE from washing where temperature is not null
    """).rdd.map(lambda row: row.temperature).#YOUR CODE GOES HERE
    if not type(my_list)==list:
        raise Exception('return type not a list')
    return my_list

"""Finally we want to create a run chart. Please return two lists (encapsulated in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refer to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm"""

#should return a tuple containing the two lists for timestamp and temperature
#please make sure you take only 10% of the data by sampling
#please also ensure that you sample in a way that the timestamp samples and temperature samples correspond (=> call sample on an object still containing both dimensions)
def getListsForRunChart():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    #https://spark.apache.org/docs/latest/api/sql/
    double_tuple_rdd = spark.sql("""
        select #YOUR CODE GOES HERE,#YOUR CODE GOES HERE from washing where temperature is not null order by ts asc
    """).#YOUR CODE GOES HERE(False,0.1).rdd.map(lambda row : (row.ts,row.temperature))
    result_array_ts = double_tuple_rdd.map(lambda ts_temperature: ts_temperature[0]).collect()
    result_array_temperature = double_tuple_rdd.map(lambda ts_temperature: ts_temperature[1]).collect()
    return (result_array_ts,result_array_temperature)

"""Now it is time to grab a PARQUET file and create a dataframe out of it. Using SparkSQL you can handle it like a database."""

!wget https://github.com/IBM/coursera/blob/master/coursera_ds/washing.parquet?raw=true
!mv washing.parquet?raw=true washing.parquet

df = spark.read.parquet('washing.parquet')
df.createOrReplaceTempView('washing')
df.show()

"""Now we gonna test the functions you've completed and visualize the data."""

# Commented out IPython magic to ensure Python compatibility.
# %matplotlib inline
import matplotlib.pyplot as plt

plt.hist(getListForHistogramAndBoxPlot())
plt.show()

plt.boxplot(getListForHistogramAndBoxPlot())
plt.show()

lists = getListsForRunChart()

plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()

"""Congratulations, you are done! The following code submits your solution to the grader. Again, please update your token from the grader's submission page on Coursera"""

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

from rklib import submitAll
import json

key = "S5PNoSHNEeisnA6YLL5C0g"
email = ###_YOUR_CODE_GOES_HERE_###
token = ###_YOUR_CODE_GOES_HERE_### #you can obtain it from the grader page on Coursera (have a look here if you need more information on how to obtain the token https://youtu.be/GcDo0Rwe06U?t=276)

parts_data = {}
parts_data["iLdHs"] = json.dumps(str(type(getListForHistogramAndBoxPlot())))
parts_data["xucEM"] = json.dumps(len(getListForHistogramAndBoxPlot()))
parts_data["IyH7U"] = json.dumps(str(type(getListsForRunChart())))
parts_data["MsMHO"] = json.dumps(len(getListsForRunChart()[0]))

submitAll(email, token, key, parts_data)
