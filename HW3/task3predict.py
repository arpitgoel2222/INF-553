from pyspark import SparkConf, SparkContext
import json
import sys
import itertools
import time
import string
import math
from itertools import combinations


sc = SparkContext(appName="hw3_task3")
sc.setLogLevel("ERROR")

train_file  = sys.argv[1]
test_file = sys.argv[2]
model_file = sys.argv[3]
output = sys.argv[4]
cf_type = sys.argv[5]


def predict(x):
    n= []
    #Chek if the pair is present in model, if yes then append first part, else append the second part of the pair(x)
    for b in x[1][1].keys():
        one = (x[1][0],b)
        two = (b,x[1][0])
        if one in m.keys():
            n.append((x[1][0],b,m[one]))
        elif two in m.keys():
            n.append((x[1][0],b,m[two]))
        
    n = sorted(n, key=lambda t:t[2],reverse=True)
    #Taking N as 3 for both cases
    #Get the first three neighbour of each item/user
    n = n[:3]
    
    num = 0
    den = 0
    #Get the final stars for each user id/busineess_id
    for y in n:
        if y[1] in x[1][1].keys():
            stars = x[1][1][y[1]]
            num += stars * y[2]
            den +=y[2]
    #Return if both are greater than zero and filter empty values later
    if den >0 and num > 0:
        return (float(num/den))

def get_rating(pair):
    #Get the average rating for each user id by aggregating ratings for each business id
    r = {}
    for p in pair:
        if p[0] not in r.keys():
            r[p[0]] = [p[1]]
        else:
            r[p[0]].append(p[1])
    #get average for each key
    for k in r.keys():
        r[k] = (sum(r[k]))/(len(r[k]))
    return r
        

train_reviews = sc.textFile(train_file).map(lambda x: json.loads(x))


if cf_type == "item_based":
    business = sc.textFile(train_file).map(lambda x: json.loads(x)).map(lambda x: (x["user_id"],(x["business_id"],x["stars"]))).groupByKey().mapValues(list)
    reviews = sc.textFile(test_file).map(lambda x: json.loads(x)).map(lambda x: (x["user_id"],x["business_id"]))
    
    m = {}
    #Load the model
    with open(model_file) as f:
        lines = f.readlines()
        for l in lines:
            l = json.loads(l.strip())
            key = (l["b1"],l["b2"])
            m[key] = l["sim"]
            
    #Get average rating for each business
    business = business.map(lambda x: (x[0],get_rating(x[1])))
    
    rating = reviews.join(business)
    #print(rating.take(1))
    #print(len(rating))
    #Predict rating anf filteer it
    rating = rating.map((lambda x: (x[0],x[1][0],predict(x))))
    rating = rating.filter(lambda x: x[2]!=0 and x[2]!=None).collect()
    
    item_based_length = len(rating)
    #print(item_based_length)
    with open(output,"w") as f:
        for i  in range(0, item_based_length):
            if i!=item_based_length-1:
                f.write(json.dumps({"user_id":rating[i][0],"business_id":rating[i][1],"stars":rating[i][2]}))
                f.write("\n")
            else:
                f.write(json.dumps({"user_id":rating[i][0],"business_id":rating[i][1],"stars":rating[i][2]}))
    #print(rating.take(2))
    
    
if cf_type == "user_based":
    business = sc.textFile(train_file).map(lambda x: json.loads(x)).map(lambda x: (x["business_id"],(x["user_id"],x["stars"]))).groupByKey().mapValues(list)
    reviews = sc.textFile(test_file).map(lambda x: json.loads(x)).map(lambda x: (x["business_id"],x["user_id"]))

    m = {}
    #Load the mmodel
    with open(model_file) as f:
        lines = f.readlines()
        for l in lines:
            l = json.loads(l.strip())
            key = (l["u1"],l["u2"])
            m[key] = l["sim"]
        
    #Get average rating for each business
    business = business.map(lambda x: (x[0],get_rating(x[1])))

    rating = reviews.join(business)
#print(rating.take(1))
#print(len(rating))
    #Predict the rating and filter it accordingly
    rating = rating.map((lambda x: (x[0],x[1][0],predict(x))))
    rating = rating.filter(lambda x: x[2]!=0 and x[2]!=None).collect()

    user_based_length = len(rating)
    #print(item_based_length)
    with open(output,"w") as f:
        for i  in range(0, user_based_length):
            if i!=user_based_length-1:
                f.write(json.dumps({"user_id":rating[i][0],"business_id":rating[i][1],"stars":rating[i][2]}))
                f.write("\n")
            else:
                f.write(json.dumps({"user_id":rating[i][1],"business_id":rating[i][0],"stars":rating[i][2]}))
#print(rating.take(2))
    
