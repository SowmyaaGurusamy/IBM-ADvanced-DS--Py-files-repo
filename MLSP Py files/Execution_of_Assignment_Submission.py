from rklib import submit
key = "-fBiYHYDEeiR4QqiFhAvkA"
part = "IjtJk"
email = ###YOUR_CODE_GOES_HERE###
submission_token = ###YOUR_CODE_GOES_HERE### 

# (have a look here if you need more information on how to obtain the token https://youtu.be/GcDo0Rwe06U?t=276)

with open('a2_m4.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, submission_token, key, part, [part], data)