from pyspark import SparkContext
import json
import csv


sc = SparkContext(appName="inf553")
sc.setLogLevel("ERROR")

file1 = "review.json"
file2 = "business.json"
output = "user_business.csv"


reviews = sc.textFile(file1)
reviews = reviews.map(lambda x: json.loads(x)).map(lambda x : (x['business_id'],x['user_id']))


business = sc.textFile(file2)
business = business.map(lambda x: json.loads(x)).filter(lambda x : x['state']=="NV").map(lambda c : (c['business_id'],1))


user_business = business.join(reviews).map(lambda x: (x[1][1],x[0])).collect()

with open(output, "w") as f:
    writer = csv.writer(f)
    writer.writerow(["user_id","business_id"])
    writer.writerows(user_business)








