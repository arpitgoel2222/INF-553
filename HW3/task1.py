from pyspark import SparkContext
import json
import sys
import itertools
import time
import random
from pyspark.rdd import RDD
from itertools import combinations


sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")


start_time = time.time()
input_file = sys.argv[1]
output_file = sys.argv[2]

def lsh(pairs):
    #Perform LSH algorithm
    users = pairs[1]
    global bands
    hashed_values= []
    counter = 0
    c=0
    #Iterate through all of the bands
    while c < bands:
        #Getting the tuple of all users within the band range and put it in list
        p = tuple(users[c:c+r])
        hashed_values.append(((counter,p),pairs[0]))
        c =c+r
        counter = counter+1
    return hashed_values
        
    
def calculate_js(x):
    #JS = intersection/union
    #irst = x[0]
    second = x[1]
    
    first_f = set(feat[x[0]])
    second_f = set(feat[x[1]])
    
    similarity =  float(len((first_f)&(second_f))) / float(len((first_f)|(second_f)))
    
    return {"b1":first,"b2":second,"sim":similarity}
    

def performhashing(pairs):
    users = pairs[1]
    global a,b,m
    values=[]
    #For each hash function generated using values of A & B
    for x in range(100):
        minimum = float('Inf')
        for user in users:
            #For each user, calculate its hash and take the min value from all of the hashed values
            uhash= ((a[x] * user + b[x]) % m)
            minimum = min(minimum,uhash)
        values.append(minimum)
    return (pairs[0],values)


#Load data and get distinct numbers of users and businesses
data = sc.textFile(input_file).map(lambda x: json.loads(x))

users = data.map(lambda x: x["user_id"]).distinct().collect()
no_of_users = len(users)
m = no_of_users

business = data.map(lambda x: x["business_id"]).distinct().collect()
no_of_business = len(business)

#Map each user id with its indexx/row number
user_dict ={}
for i in range(0,no_of_users):
    user_dict[users[i]] = i
    
#Get user and business id together and reduce it
businesses = data.map(lambda x: (x['business_id'],x['user_id'])).distinct().map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y)

#Get features of each business
feat = businesses.collectAsMap()

#Map user id with its index
businesses = businesses.map(lambda x: (x[0], [user_dict[i] for i in x[1]]))
#print(businesses.take(2))

#Generated these values using random function and then fixed it
a = [21906, 23417, 6849, 410, 3880, 10610, 4441, 8449, 22990, 16887, 4212, 24962, 8449, 12581, 1968, 17393, 2153, 97, 15719, 7404, 26121, 6306, 16636, 6968, 22183, 4412, 24639, 20214, 21002, 11519, 22013, 19571, 6338, 23985, 18814, 9679, 21934, 15721, 22395, 25810, 2238, 1490, 17029, 21369, 25334, 14920, 7865, 25026, 20041, 18145, 14434, 8337, 24473, 4028, 11395, 20407, 7174, 16471, 1164, 5825, 24296, 5759, 22789, 23197, 4822, 10221, 5211, 20695, 18597, 5187, 21460, 21232, 3023, 22351, 23739, 7635, 1137, 14164, 22614, 14003, 21378, 15609, 12982, 5886, 20194, 13406, 17023, 11587, 6400, 10174, 11870, 21386, 2625, 3709, 24995, 4636, 8411, 9014, 7099, 24044, 3555, 7527, 4199, 10761, 10188, 6134, 15568,21603, 26037, 25734, 23296, 23401, 10770, 594, 11775, 9280, 24062, 22975, 2843, 11910, 4236, 9343, 19385, 12126, 10528, 5063, 1718, 276, 415, 24955, 24178, 13352, 8613, 1637, 25891, 8101, 4515, 4309, 417, 12762, 274, 5461, 23441, 13807, 13877, 13039, 21734, 8708, 102, 22585]

b= [2865, 3227, 5742, 2978, 23160, 15195, 18928, 16621, 12859, 6974, 20670, 6473, 4230, 3075, 3998, 8021, 6802, 10126, 3624, 21848, 16557, 23680, 8320, 9189, 19543, 14751, 26040,7196, 2181, 9842, 161, 1537, 1948, 10545, 19199, 25653, 7996, 4012, 25409, 6212, 823, 24863, 12336, 4222, 17267, 24540, 8025, 10300, 11070, 16859, 11145, 22674, 6614, 23084, 23308, 944, 15035, 19945, 11673, 24936, 3686, 14526, 15041, 6588, 12425, 5423, 17651, 5252, 6650, 23944, 13667, 19279, 19692, 21294, 5028, 18497, 20467, 6597, 19230, 14314, 12627, 16759, 19398, 17713, 6635, 9522, 8095, 12028, 20055, 22463, 21796, 18659, 25726, 1734, 19441, 1419, 10354, 24513, 15015, 19417]

#Hash the user id to get the signature matrix
signature_matrix = businesses.map(performhashing);

#LSH initialization STEP
bands = 100
r = 1

#Perform LSH on the signature matrix and get pairs which are common in atleast one band
candidates = signature_matrix.flatMap(lsh).map(lambda x: (x[0],[x[1]])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0],list(set(x[1])))).filter(lambda x: len(x[1]) > 1)

intermediate = candidates.map(lambda x: list(combinations(x[1],2))).flatMap(lambda x: x).distinct()

#Calculate jaacard similarity and get pairs with js greater than 0.05
sim_pairs = intermediate.map(calculate_js).filter(lambda x: x["sim"] >= 0.05).collect()

no_of_sim_pairs = len(sim_pairs)

with open(output_file,"w") as f:
    for i in range(0, no_of_sim_pairs):
        if i != no_of_sim_pairs-1:
            f.write(json.dumps(sim_pairs[i]))
            f.write("\n")
        else:
            f.write(json.dumps(sim_pairs[i]))
            
            
            






