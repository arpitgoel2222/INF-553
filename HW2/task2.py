
from pyspark import SparkContext
import json
import itertools
import time
import sys
import math
from operator import add
import collections


sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

k = int(sys.argv[1])
support = float(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]


def map2(data_baskets, candidate_pairs):
    temp = collections.defaultdict(list)
    for pairs in candidate_pairs:
        if set(pairs).issubset(set(data_baskets)):
            temp[pairs].append(1)
    
    items = []
    #for each item, count all its count
    for k ,v in temp.items():
        items.append(tuple((k, sum(v))))
    return items

def getmorepairs(baskets,prev_freq_items,k):
    itemsets = {}
    freq_itemsets = []
    candidates = []
    #for each item generated from the combination check two item intersection should always have k-2 elements, if yes combine both sets to geet a new set of size k
    #Ex: [1,2] and [2,3] and has [2] as & and [1,2,3] as |
    for item in itertools.combinations(prev_freq_items, 2):
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

def apriori(chunk):
    result = []
    #print("-------")
    basket=[]
    baskets_in_partition = list(chunk)
    basket_length = len(baskets_in_partition)
    threshold = float(support * (float(basket_length)/float(basket_count)))
    
    for b in baskets_in_partition:
        basket.append(set(b[1]))
    
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
    #print(len(single_items))
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
    #print(len(pair_frequent))
    
    
    k=3
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
data = sc.textFile(input_file).map(lambda x: x.split(",")).filter(lambda x: len(x)>1).map(lambda x: (x[0],x[1]))
data = data.filter(lambda x: x[0]!= "user_id" and x[1]!= "business_id").distinct()

data = data.groupByKey().map(lambda x: (x[0],list(x[1])))

data = data.filter(lambda x : len(x[1]) > k)
data2 = data.map(lambda x : x[1])

total = data.collect()
basket_count = len(total)
#print(basket_count)
candidates = data.mapPartitions(apriori).distinct()
curr_freq_itemsets = candidates.collect()

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



result = data2.flatMap(lambda basket:map2(basket,ans1)).reduceByKey(lambda x,y: x+y)
result = result.filter(lambda x:x[1] >= support).sortBy(keyfunc= lambda x: (len(x[0]),x[0])).keys().collect()
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
