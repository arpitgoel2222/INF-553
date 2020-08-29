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

input_file  = sys.argv[1]
output_model_file = sys.argv[2]
cf_type = sys.argv[3]


def lsh(pairs):
    #Perform lsh as in task1
    users = pairs[1]
    global bands
    hashed_values= []
    counter = 0
    c=0
    while c < bands:
        #Get the pairs within the band
        p = tuple(users[c:c+r])
        hashed_values.append(((counter,p),pairs[0]))
        c =c+r
        counter = counter+1
    return hashed_values
        
    
def calculateavgstars(pair):
    users= {}
    #Claculate the average no of stars for each user id aggregated by their business id
    for x in pair:
        if x in users.keys():
            users[x[0]][0] += x[1]
            users[x[0]][1] += 1
        else:
            users[x[0]] = [x[1],1]
    for x in users.keys():
        users[x] = float(users[x][0])/float(users[x][1])
    return users
    
def calculate_js(x):
    #JS= Intersection/union
    first = x[0]
    second = x[1]
    
    first_f = set(feat[first])
    second_f = set(feat[second])
    
    similarity =  float(len((first_f)&(second_f))) / float(len((first_f)|(second_f)))
    
    return {"u1":first,"u2":second,"sim":similarity}

def performhashing(pairs):
    #Perform hashing as in task1
    values=[]
    hashed_values = []
    p = pairs[1]
    global no_of_business
    m = no_of_business
    global a,b


    for i in range(len(a)):
        h = list(map((lambda x : ((((a[i]*x) + b[i])) % m)),p))
        hashed_values.append(min(h))
    #print(hashed_values)
    return (pairs[0],hashed_values)

def process_business_id(business_pair):
    #Claculate the average no of stars for each user id aggregated by their business id
    users = {}
    for x in business_pair:
        #print(x)
        if x[0] not in users.keys():
            users[x[0]] = [x[1],1]
        else:
            users[x[0]][0] += x[1]
            users[x[0]][1] += 1
    #print(users)
    
    for x in users.keys():
        users[x] = float(users[x][0])/float(users[x][1])
    return users
            
    


if cf_type=="item_based":
    #print("---")
    reviews = sc.textFile(input_file).map(lambda x: json.loads(x)).map(lambda x: (x["business_id"],[(x["user_id"],x["stars"])])).reduceByKey(lambda x,y : x + y)
    #print(reviews.take(1))
    #reviews = reviews.distinct()
    
    #get the average number of stars for each user id
    businesses = reviews.mapValues(process_business_id).collectAsMap()
    
    #Number of businesses and all combinations of it
    business = businesses.keys()
    #sample = businesses[:5]
    relations = list(combinations(business,2))
    
    #Pearson correlation using the average corated ratings
    output = []
    for x in relations:
        first = x[0]
        second = x[1]
        #First find number of corated users using intersection for two users
        user1 = businesses[first]
        user2 = businesses[second]
        i=0
        no_of_corated_users = list(set(user1)&(set(user2)))
        #print(len(no_of_corated_users))
        if len(no_of_corated_users)>=3:
            sum1 = 0
            sum2 = 0
            #Get ratings for each business id associated
            for user in no_of_corated_users:
                sum1 += businesses[first][user]
                sum2 += businesses[second][user]
            #Get average for each business
            avg1 = float(float(sum1)/float(len(no_of_corated_users)))
            avg2 = float(float(sum2)/float(len(no_of_corated_users)))
            upper = 0
            bd1 = 0
            bd2 = 0
            for user in no_of_corated_users:
                #First get rating of each user
                r1 = user1[user]
                r2 = user2[user]
                #Get normalized ratings
                diff1 = r1-avg1
                diff2 = r2-avg2
                upper += diff1*diff2
                bd1 += diff1 * diff1
                bd2 += diff2 * diff2
            #Modulus function
            down = math.sqrt(bd1) * math.sqrt(bd2)
            
            if upper >=0:
                if down>0:
                    rating = float(upper)/float(down)
                    output.append((x,rating))
                    #print(output)
    print(len(output))
    no_of_pairs = len(output)
    
    with open(output_model_file,"w") as f:
        for i in range(0,no_of_pairs):
            ans = {}
            if i!=no_of_pairs-1:
                f.write(json.dumps({"b1":output[i][0][0],"b2":output[i][0][1],"sim":output[i][1]}))
                f.write("\n")
            
            else:
                f.write(json.dumps({"b1":output[i][0][0],"b2":output[i][0][1],"sim":output[i][1]}))
            
            
if cf_type=="user_based":
    data = sc.textFile(input_file).map(lambda x: json.loads(x))

    #Get distinct no of users and businesses
    users = data.map(lambda x: x["user_id"]).distinct().collect()
    no_of_users = len(users)
    #print(no_of_users)
    m = no_of_users

    business = data.map(lambda x: x["business_id"]).distinct().collect()
    no_of_business = len(business)

    #Map each user id and business id to a index respectively
    user_dict ={}
    for i in range(0,no_of_users):
        user_dict[users[i]] = i
        
    bus_dict ={}
    for i in range(0,no_of_business):
        bus_dict[business[i]] = i
        
    businesses = data.map(lambda x: (x['user_id'],x['business_id'])).distinct().map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y)
    
    #Calculate average numbers of stars for each user id for its all user id
    b1 = data.map(lambda x: (x['user_id'],(x['business_id'],x["stars"]))).distinct().map(lambda x: (x[0],[(x[1][0],x[1][1])])).reduceByKey(lambda a, b: a + b).mapValues(calculateavgstars).collectAsMap()

    feat = businesses.collectAsMap()
    
    #Geenerated using the random functiona and then fixed it
    a = [7978, 9024, 1970, 1264, 9919, 5703, 664, 4738, 6694, 6971, 1642, 7882, 4494, 3340, 5220, 6497, 7055, 6873, 3473, 5021, 6990, 5918, 9841, 7616, 8398, 1891, 8494, 3854, 1500, 153, 6523, 7403, 9888, 4157, 3591, 5629, 2003, 7585, 8946, 6411, 2647, 7844, 6288, 9509, 5168, 6841, 9641, 9447, 9184, 789, 3114, 9941, 5223, 9877, 2630, 9311, 6805, 9394, 5569, 6082, 4464, 9857, 7096, 8526, 10140, 5820, 105, 1736, 6950, 8921, 8184, 7445, 9378, 6966, 1390, 1455, 7765, 7737, 6986, 6664, 6640, 3601, 7135, 6390, 8776, 8118, 1916, 227, 5683, 3420, 3586, 9534, 7514, 3569, 1248, 3472, 6878, 8038, 4625, 4999]
    
    b = [3872, 949, 509, 5914, 2699, 7243, 3448, 313, 7276, 8331, 9564, 4060, 3303, 6482, 343, 4641, 2382, 8876, 8554, 6338, 7672, 2408, 3124, 573, 8125, 5049, 5306, 9853, 2967, 3952, 9113, 5132, 8044, 4212, 5341, 952, 1964, 4865, 4053, 6232, 360, 10056, 9320, 6424, 6114, 1415, 7775,3653, 2602, 2473, 6040, 8269, 9811, 5251, 1573, 3649, 8657, 2618, 6374, 7295, 7060, 5803, 9463, 5599, 1058, 8898, 231, 1287, 542, 2152, 2538, 4459, 6267, 1151, 6097, 4174, 1308, 2462, 2932, 3576, 4802, 7672, 3330, 5861, 3321, 5322, 1387, 6808, 2568, 7512, 5804, 2257, 3314, 7371, 1078, 6800, 1913, 8246, 2389, 4449]

    #Map businesses with its index
    businesses = businesses.map(lambda x: (x[0], [bus_dict[i] for i in x[1]]))
    signature_matrix = businesses.map(performhashing);
        
    bands = 20
    r = 1

    #Perform lsh and find the pairs that falls in atleast one common band
    candidates = signature_matrix.flatMap(lsh).map(lambda x: (x[0],[x[1]])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0],list(set(x[1])))).filter(lambda x: len(x[1]) > 1)
    intermediate = candidates.map(lambda x: list(combinations(x[1],2))).flatMap(lambda x: x).distinct()

    #Calculate jaccard similarity and filter it accordingly
    sim_pairs = intermediate.map(calculate_js).filter(lambda x: x["sim"] >= 0.01).map(lambda x: (x['u1'],x['u2'])).collect()

    #Pearson Correlation using the aveerage corated rating
    output = []
    for x in sim_pairs:
        first = x[0]
        second = x[1]
        
        user1 = b1[first]
        user2 = b1[second]
        i=0
        #First find number of corated users using intersection for two users
        no_of_corated_users = list(set(user1)&(set(user2)))
        #print(len(no_of_corated_users))
        if len(no_of_corated_users)>=3:
            sum1 = 0
            sum2 = 0
            for user in no_of_corated_users:
                sum1 += b1[first][user]
                sum2 += b1[second][user]
            #find average of ratings for each user
            avg1 = float(float(sum1)/float(len(no_of_corated_users)))
            avg2 = float(float(sum2)/float(len(no_of_corated_users)))
            upper = 0
            bd1 = 0
            bd2 = 0
            for user in no_of_corated_users:
            #Get users rating
                r1 = user1[user]
                r2 = user2[user]
            #get normalized rating
                diff1 = r1-avg1
                diff2 = r2-avg2
                upper += diff1*diff2
                bd1 += diff1 * diff1
                bd2 += diff2 * diff2
            #modulus
            down = math.sqrt(bd1) * math.sqrt(bd2)
            
            if upper >=0:
                if down>0:
                    rating = float(upper)/float(down)
                    output.append((x,rating))

    no_of_pairs = len(output)
    
    with open(output_model_file,"w") as f:
        for i in range(0,no_of_pairs):
            ans = {}
            if i!=no_of_pairs-1:
                f.write(json.dumps({"u1":output[i][0][0],"u2":output[i][0][1],"sim":output[i][1]}))
                f.write("\n")
            
            else:
                f.write(json.dumps({"u1":output[i][0][0],"u2":output[i][0][1],"sim":output[i][1]}))
    
               
#
#
#
#
#
#
#[7978, 9024, 1970, 1264, 9919, 5703, 664, 4738, 6694, 6971, 1642, 7882, 4494, 3340, 5220, 6497, 7055, 6873, 3473, 5021, 6990, 5918, 9841,
#7616, 8398, 1891, 8494, 3854, 1500, 153, 6523, 7403, 9888, 4157, 3591, 5629, 2003, 7585, 8946, 6411, 2647, 7844, 6288, 9509, 5168, 6841, 9
#641, 9447, 9184, 789, 3114, 9941, 5223, 9877, 2630, 9311, 6805, 9394, 5569, 6082, 4464, 9857, 7096, 8526, 10140, 5820, 105, 1736, 6950, 89
#21, 8184, 7445, 9378, 6966, 1390, 1455, 7765, 7737, 6986, 6664, 6640, 3601, 7135, 6390, 8776, 8118, 1916, 227, 5683, 3420, 3586, 9534, 751
#4, 3569, 1248, 3472, 6878, 8038, 4625, 4999]
#[3872, 949, 509, 5914, 2699, 7243, 3448, 313, 7276, 8331, 9564, 4060, 3303, 6482, 343, 4641, 2382, 8876, 8554, 6338, 7672, 2408, 3124, 573
#, 8125, 5049, 5306, 9853, 2967, 3952, 9113, 5132, 8044, 4212, 5341, 952, 1964, 4865, 4053, 6232, 360, 10056, 9320, 6424, 6114, 1415, 7775,
# 3653, 2602, 2473, 6040, 8269, 9811, 5251, 1573, 3649, 8657, 2618, 6374, 7295, 7060, 5803, 9463, 5599, 1058, 8898, 231, 1287, 542, 2152, 2
#538, 4459, 6267, 1151, 6097, 4174, 1308, 2462, 2932, 3576, 4802, 7672, 3330, 5861, 3321, 5322, 1387, 6808, 2568, 7512, 5804, 2257, 3314, 7
#371, 1078, 6800, 1913, 8246, 2389, 4449]
