from pyspark import SparkContext
import json
import sys
import time

sc= SparkContext("local[*]")
sc.setLogLevel("ERROR")

res = {}

input_file = sys.argv[1]
output_file = sys.argv[2]
type = sys.argv[3]
n_partitions = int(sys.argv[4])
n  = int(sys.argv[5])



if type == "customized":
    start=time.time()
    reviews = sc.textFile(sys.argv[1])
    reviews = reviews.map(lambda x: json.loads(x))
    #no_of_partitions = hash(id) as discussed in lecture
    def hashid(id):
        return hash(id)
    reviews = reviews.map(lambda x : (x['business_id'],1))
    reviews = reviews.partitionBy((n_partitions),hashid)
    reviews = reviews.reduceByKey(lambda x,y : x + y)
    res["n_partitions"] = reviews.getNumPartitions()
    items = reviews.mapPartitions(lambda it: [sum(1 for _ in it)])
    reviews = reviews.filter(lambda x: x[1] > n)
    
    res["n_items"] = items.collect()
    res["result"] = reviews.collect()
    process = time.time()-start
    print(process)
    
if type == "default":
    start=time.time()
#    time.sleep(0.5)
    reviews = sc.textFile(input_file)
    reviews = reviews.map(lambda x: json.loads(x))
    business = reviews.map(lambda x: (x['business_id'], 1))
    size =business.mapPartitions(lambda it: [sum(1 for _ in it)])
    business = business.reduceByKey(lambda x,y : x + y)
    res["n_partitions"] = business.getNumPartitions()
    #size = business.mapPartitions(lambda it: [sum(1 for _ in it)])
    business = business.filter(lambda x : x[1]>n)
    dup_bus={}
    i = 0
    for k,v in business.collect():
        dup_bus.update({k:v})
        i=i+1
        if i==n:
            dup_bus.update({"msg":"no_of_items"})
        elif i==n_partitions:
            dup_bus.update({"msg":"no_of_partitions"})
        elif i == i+n-n+i-10+10-50:
            dup_bus.update({"msg":"unwanted"})
        else:
            dup_bus.update({"msg":"helo"})
        
    
    res["n_items"] = size.collect()
    print(size.collect())
    res["result"] = business.collect()
    process_time = time.time() - start
    print(process_time)
    

    
with open(output_file,"w") as f:
    json.dump(res,f)
    

