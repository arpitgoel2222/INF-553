from pyspark import SparkContext
import json
import itertools
import time
import sys
import math

sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

case_number = sys.argv[1]
support = float(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]


def getmorepairs(baskets,prev_freq_items,k):
    itemsets = {}
    freq_itemsets = []
    candidates = []
    for item in itertools.combinations(prev_freq_items, 2):
    #for each item generated from the combination check two item intersection should always have k-2 elements, if yes combine both sets to geet a new set of size k
    #Ex: [1,2] and [2,3] and has [2] as & and [1,2,3] as |
        if len(set(item[0])&(set(item[1]))) == k-2:
            first = set(item[0])
            second = set(item[1])
            pair  = sorted(first|second)
            candidate = tuple(pair)
            if candidate not in candidates:
            #from the new set generated , check whether its immediate subsets(k-1) are part of frequent pairs or not, if all of them are part of frequent set,
            #new set will also be a frequent itemset, add it to the list
            #[1,2],[1,3] and [2,3] should be frequent as well
                temp = itertools.combinations(candidate,k-1)
                if set(temp).issubset(prev_freq_items):
                    candidates.append(candidate)

    return candidates
    

def map2(chunk,frequent_itemsets):
    baskets = chunk.collect()
    result = []
    #Map2 will check if the frequent item is frequent in the whole dataset or not
    for itemset in frequent_itemsets:
        if itemset not in result:
            count = 0
            #Check if the frequent pair we got from map1 is in the initial basket or not,if yes store it with its count and later filter it on the basis of the threshold
            for basket in baskets:
                #for eevery tuple, check all its subset
                if type(itemset) is tuple:
                    if set(itemset).issubset(set(basket)):
                        count += 1
                else:
                    if itemset in basket :
                        count += 1
            if count >= support:
                result.append(itemset)
    
    return result

def apriori(chunk):
    result = []
    #print("-------")
    basket = list(chunk)
    basket_length = len(basket)
    #Note: Taking the thershold as ceiling value
    threshold = math.ceil(support * (float(basket_length)/float(basket_count)))
    
    #Singletons
    items= {}
    #count occurence of each single items in the basket and store it with it count and later filter it on the basis of the threshold
    for x in basket:
        for y in x:
            if y in items:
                items[y] += 1
            else:
                items[y] = 1

    freq_single_items = []
    single_items=[]
    for k,v in items.items():
        if v >= threshold:
            freq_single_items.append(k)
            single_items.append((k,))

    freq_single_items.sort()
    
    result += single_items
    result =sorted(result)
    
    #Doublets
    #Generate Doublets using the singletons
    candidates=[]
    for itemset in itertools.combinations(freq_single_items,2):
        candidates.append(itemset)

    
    #Check if doublet can be found in the initial basket, if yes store it with its count and later filter it on the basis of the threshold
    double={}
    for candidate in candidates:
            for x in basket:
                if set(candidate).issubset(x):
                    double.setdefault(candidate,0)
                    double[candidate] += 1

    pair_frequent=[]
    for item,count in double.items():
        if count >= threshold:
            pair_frequent.append(item)

    result+= pair_frequent
    

    #Triplets and further
    #Starting with k=3 for triplets. The loop will go on till all the singletons are covered as each will generate sets with other
    k=3
    #till new set generated is empty
    while True:
        prev_freq_items = pair_frequent
    
        candidates = getmorepairs(basket,prev_freq_items,k);
        #Check if pairs can be found in the initial basket as done above for doublets, if yes store it with its count and later filter it on the basis of the threshold
        double={}
        for candidate in candidates:
                for x in basket:
                    if set(candidate).issubset(x):
                        double.setdefault(candidate,0)
                        double[candidate] += 1

        pair_frequent=[]
        for item,count in double.items():
            if count >= threshold:
                pair_frequent.append(item)
        #print(len(pair_frequent))
    
        if pair_frequent==[]:
            break;
    
        result += pair_frequent
    
        k += 1
        
    return result
        
    
    
start_time = time.time()
data = sc.textFile(input_file)
header = data.first()
data= data.filter(lambda x: x != header).map(lambda x: tuple(x.split(",")))

if case_number == "1":
    rdd = data.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x + y)
    
elif case_number == "2":
    rdd = data.map(lambda x: (x[1],[x[0]])).reduceByKey(lambda x,y: x + y)


#Group each user/business with business/user
basket = rdd.map(lambda x: (x[0],list(set(x[1]))))
basket_count = basket.values().count()
baskets = basket.map(lambda x: x[1])
#print(baskets.take(2))

candidates = baskets.mapPartitions(apriori).distinct()
#Sort the candidates in lexxiographical order
ans1 = candidates.map(lambda itemsets: (itemsets,1)).sortBy(keyfunc= lambda x: (len(x[0]),x[0])).keys().collect()
print(len(ans1))

start=1
f = open(output_file, 'w')
f.write("Candidates: \n")

while start!= basket_count:
    pair = ""
    for x in ans1:
        if(len(x)==start):
            pair = pair + str(x).replace(",)",")")
            pair = pair.replace(")(","),(")


    if(pair !=""):
        if(start!=1):
            f.write("\n\n")
        f.write(pair)
    start = start+1


result = map2(baskets,candidates.collect())
result = sorted(result)
print(len(result))
start=1
f = open(output_file, 'a')
f.write("\n\nFrequent Itemsets: \n")
while start!= basket_count:
    pair = ""
    for x in result:
        if(len(x)==start):
            pair = pair + str(x).replace(",)",")")
            pair = pair.replace(")(","),(")


    if(pair !=""):
        if(start!=1):
            f.write("\n\n")
        f.write(pair)
    start = start+1
    
print("Duration: %s" % (time.time() - start_time))









    






