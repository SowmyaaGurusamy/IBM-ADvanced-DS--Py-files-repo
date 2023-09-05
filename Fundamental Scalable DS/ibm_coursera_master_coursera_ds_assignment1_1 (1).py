# -*- coding: utf-8 -*-
"""IBM_coursera_master_coursera_ds_assignment1.1.ipynb



# Warmup Assignment

In this exercise you will just submit a ping to the grader in order to make sure the exercise and grading environment is setup correctly.

We have to install a little library in order to submit to coursera
"""

!rm -f rklib.py
!wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py

"""Please provide your email address and obtain a submission token on the grader’s submission page in coursera, then execute the cell"""

from rklib import submit
import json

key = "RrIb4SHNEeiLcw7AkKxwaA"
part = "zetaj"
email = ###_YOUR_CODE_GOES_HERE_###
token = ###_YOUR_CODE_GOES_HERE_### #you can obtain it from the grader page on Coursera (have a look here if you need more information https://youtu.be/GcDo0Rwe06U?t=276)


submit(email, token, key, part, [part], json.dumps(23))
