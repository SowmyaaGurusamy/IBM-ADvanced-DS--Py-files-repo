# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ds_assignment2.1_spark2.3_python3.6.ipynb


### Assignment 2
Welcome to Assignment 2. This will be fun. It is the first time you actually access external data from ApacheSpark.

#### You can also submit partial solutions

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

"""This is the first function you have to implement. You are passed a dataframe object. We've also registered the dataframe in the ApacheSparkSQL catalog - so you can also issue queries against the "washing" table using "spark.sql()". Hint: To get an idea about the contents of the catalog you can use: spark.catalog.listTables().
So now it's time to implement your first function. You are free to use the dataframe API, SQL or RDD API. In case you want to use the RDD API just obtain the encapsulated RDD using "df.rdd". You can test the function by running one of the three last cells of this notebook, but please make sure you run the cells from top to down since some are dependant of each other...
"""

#Please implement a function returning the number of rows in the dataframe
def count():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL
    #some more help: https://www.w3schools.com/sql/sql_count_avg_sum.asp
    return spark.sql('select ### as cnt from washing').first().cnt

"""Now it's time to implement the second function. Please return an integer containing the number of fields (columns). The most easy way to get this is using the dataframe API. Hint: You might find the dataframe API documentation useful: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html"""

def getNumberOfFields():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL
    return len(df.###)

"""Finally, please implement a function which returns a (python) list of string values of the field names in this data frame. Hint: Just copy&past doesn't work because the auto-grader will create a random data frame for testing, so please use the data frame API as well. Again, this is the link to the documentation: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL"""

def getFieldNames():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Working-with-SQL
    return df.###

"""Now it is time to grab a PARQUET file and create a dataframe out of it. Using SparkSQL you can handle it like a database."""

!wget https://github.com/IBM/coursera/blob/master/coursera_ds/washing.parquet?raw=true
!mv washing.parquet?raw=true washing.parquet

df = spark.read.parquet('washing.parquet')
df.createOrReplaceTempView('washing')
df.show()

"""The following cell can be used to test your count function"""

cnt = None
nof = None
fn = None

cnt = count()
print(cnt)

"""The following cell can be used to test your getNumberOfFields function"""

nof = getNumberOfFields()
print(nof)

"""The following cell can be used to test your getFieldNames function"""

fn = getFieldNames()
print(fn)

"""Congratulations, you are done. So please submit your solutions to the grader now.

# Start of Assignment-Submission

The first thing we need to do is to install a little helper library for submitting the solutions to the coursera grader:

"""

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

"""Now it’s time to submit first solution. Please make sure that the token variable contains a valid submission token. You can obtain it from the coursera web page of the course using the grader section of this assignment.

Please specify you email address you are using with cousera as well.

"""

from rklib import submit, submitAll
import json

key = "SVDiVSHNEeiDqw70MIp2vA"

if type(23) != type(cnt):
    raise ValueError('Please make sure that "cnt" is a number')

if type(23) != type(nof):
    raise ValueError('Please make sure that "nof" is a number')

if type([]) != type(fn):
    raise ValueError('Please make sure that "fn" is a list')

email = #### your code here ###
token = #### your code here ### (have a look here if you need more information on how to obtain the token https://youtu.be/GcDo0Rwe06U?t=276)

parts_data = {}
parts_data["2FjQw"] = json.dumps(cnt)
parts_data["j8gMs"] = json.dumps(nof)
parts_data["xaauC"] = json.dumps(fn)


submitAll(email, token, key, parts_data)
