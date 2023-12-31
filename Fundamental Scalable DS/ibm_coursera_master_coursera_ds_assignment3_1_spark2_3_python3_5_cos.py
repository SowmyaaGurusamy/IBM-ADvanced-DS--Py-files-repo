# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ds_assignment3.1_spark2.3_python3.5_cos.ipynb


# Assignment 3

Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures.

## You only have to pass 4 out of 7 functions

Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
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

"""All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. But we discurage using RDD at this point in time.

Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. Everything can be implemented using SQL only if you like.
"""

def minTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL
    return spark.sql("SELECT ##INSERT YOUR CODE HERE##(temperature) as mintemp from washing").first().mintemp

"""Please now do the same for the mean of the temperature"""

def meanTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL
    return spark.sql("SELECT ##INSERT YOUR CODE HERE##(temperature) as meantemp from washing").first().meantemp

"""Please now do the same for the maximum of the temperature"""

def maxTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL
    return spark.sql("SELECT ##INSERT YOUR CODE HERE##(temperature) as maxtemp from washing").first().maxtemp

"""Please now do the same for the standard deviation of the temperature"""

def sdTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL
    #https://spark.apache.org/docs/2.3.0/api/sql/
    return spark.sql("SELECT ##INSERT YOUR CODE HERE##_pop(temperature) as sdtemp from washing").first().sdtemp

"""Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four positions in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string."""

def skewTemperature():
    return spark.sql("""
SELECT
    (
        1/##INSERT YOUR CODE HERE##
    ) *
    SUM (
        POWER(##INSERT YOUR CODE HERE##-%s,3)/POWER(%s,3)
    )

as sktemperature from washing
                    """ %(meanTemperature(),sdTemperature())).first().sktemperature

"""Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different."""

def kurtosisTemperature():
        return spark.sql("""
SELECT
    (
        1/##INSERT YOUR CODE HERE##
    ) *
    SUM (
        POWER(##INSERT YOUR CODE HERE##-%s,4)/POWER(%s,4)
    )
as ktemperature from washing
                    """ %(meanTemperature(),sdTemperature())).first().ktemperature

"""Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs."""

def correlationTemperatureHardness():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    #https://spark.apache.org/docs/2.3.0/api/sql/
    return spark.sql("SELECT ##INSERT YOUR CODE HERE##(temperature,hardness) as temperaturehardness from washing").first().temperaturehardness

"""Now it is time to grab a PARQUET file and create a dataframe out of it. Using SparkSQL you can handle it like a database."""

!wget https://github.com/IBM/coursera/blob/master/coursera_ds/washing.parquet?raw=true
!mv washing.parquet?raw=true washing.parquet

df = spark.read.parquet('washing.parquet')
df.createOrReplaceTempView('washing')
df.show()

"""Now let's test the functions you've implemented"""

min_temperature = 0
mean_temperature = 0
max_temperature = 0
sd_temperature = 0
skew_temperature = 0
kurtosis_temperature = 0
correlation_temperature = 0

min_temperature = minTemperature()
print(min_temperature)

mean_temperature = meanTemperature()
print(mean_temperature)

max_temperature = maxTemperature()
print(max_temperature)

sd_temperature = sdTemperature()
print(sd_temperature)

skew_temperature = skewTemperature()
print(skew_temperature)

kurtosis_temperature = kurtosisTemperature()
print(kurtosis_temperature)

correlation_temperature = correlationTemperatureHardness()
print(correlation_temperature)

"""Congratulations, you are done, please submit this notebook to the grader.
We have to install a little library in order to submit to coursera first.

Then, please provide your email address and obtain a submission token on the grader’s submission page in coursera, then execute the subsequent cells

### Note: We've changed the grader in this assignment and will do so for the others soon since it gives less errors
This means you can directly submit your solutions from this notebook
"""

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

from rklib import submitAll
import json

key = "Suy4biHNEeimFQ479R3GjA"
email = ###_YOUR_CODE_GOES_HERE_###
token = ###_YOUR_CODE_GOES_HERE_### #you can obtain it from the grader page on Coursera

parts_data = {}
parts_data["FWMEL"] = json.dumps(min_temperature)
parts_data["3n3TK"] = json.dumps(mean_temperature)
parts_data["KD3By"] = json.dumps(max_temperature)
parts_data["06Zie"] = json.dumps(sd_temperature)
parts_data["Qc8bI"] = json.dumps(skew_temperature)
parts_data["LoqQi"] = json.dumps(kurtosis_temperature)
parts_data["ehNGV"] = json.dumps(correlation_temperature)



submitAll(email, token, key, parts_data)
