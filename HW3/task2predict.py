from pyspark import SparkConf, SparkContext
import json
import sys
import itertools
import time
import string
import math

conf = (SparkConf().setAppName("inf553").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g"))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


def findsimilarity(pair):
    #Finding similarity only for those user id and business id which are present in the model(profiles)
    #Cosine similarity = lenegth of Dot Product/length of both profiles
    if pair["user_id"] in user_profiles.keys() and pair["business_id"] in business_profiles.keys():
        length = len(business_profiles[pair["business_id"]])
        up = len(set(business_profiles[pair["business_id"]]) & set(user_profiles[pair["user_id"]]))
        down1 = math.sqrt(len(business_profiles[pair["business_id"]]))
        down2 = math.sqrt(len(user_profiles[pair["user_id"]]))
        down = down1 * down2
        similarity = float(float(up)/float(down))
        #Filter pairs which have sim greater than 0.01
        if similarity>=0.01:
            return ((pair["user_id"],pair["business_id"]),similarity)


test_file  = sys.argv[1]
model_file = sys.argv[2]
output_file = sys.argv[3]

data = sc.textFile(test_file)
data = data.map(lambda x: json.loads(x))



model = None
with open(model_file) as f:
    model = json.load(f)
    
#Get the profiles from the model
business_profiles = model[0]
#print(len(business_profiles))
user_profiles = model[1]
#print(len(user_profiles))


result = data.map(findsimilarity).filter(lambda x: x!=None)
#print(result.take(1))
answer = result.collect()


with open(output_file, 'w') as f:

        
    for i in range(0, len(answer)):
##        if i==0:
##            print(answer[i])
##            print(answer[i][0][0])
##            print(answer[i][0][1])
##            print(answer[i][1])
        if i != len(answer)-1:
            f.write(json.dumps({"user_id":str(answer[i][0][0]),"business_id":str(answer[i][0][1]),"sim":answer[i][1]}))
            f.write("\n")
            #print("-----")
        else:
            f.write(json.dumps({"user_id":str(answer[i][0][0]),"business_id":str(answer[i][0][1]),"sim":answer[i][1]}))

#print(len(result))
