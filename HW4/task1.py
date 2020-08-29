from pyspark import SparkContext
import json
import sys
import time
from pyspark.sql import SQLContext
import os
import pyspark
from graphframes import *
import itertools
from itertools import combinations
import collections

os.environ["PYSPARK_SUBMIT_ARGS"] = (
"--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

threshold = int(sys.argv[1])
input_file = sys.argv[2]
output_file = sys.argv[3]

sc = SparkContext('local[*]', 'Task4.1')
sqlContext = SQLContext(sc)
sc.setLogLevel("ERROR")


data = sc.textFile(input_file).map(lambda x: x.split(',')).filter(lambda x: x[0] != 'user_id')
#Get the user id and business id.
#Get all the combibntains of user id possible within the business id
#Get count of all the pairs
data = data.map(lambda x: [x[1],x[0]]).groupByKey().mapValues(sorted).mapValues(lambda x: combinations(x, 2)).flatMap(lambda x: x[1]).flatMap(lambda x: [[x, 1], [x[::-1],1]]).reduceByKey(lambda x,y:x+y)

#Filter the count on the basis of threshold
data = data.map(lambda x: [x[0][0],x[0][1], x[1]]).filter(lambda x: x[2]>=threshold)

#Geet the total no of edges
no_of_edges = data.count()
print(no_of_edges)

#get all the distinct users/nodes in the graph
vertices = data.flatMap(lambda x: x[:2]).distinct().map(lambda x: [x])

#create a dataframe to store the vertices
vertices = sqlContext.createDataFrame(vertices, ['id'])

#Get the total no of nodes in the graph
no_of_nodes = vertices.count()
print(no_of_nodes)

#create a dataframe to store all the edges
edges = sqlContext.createDataFrame(data, ["src", "dst", "relation"])

#Pass vertices and edges into the graph frame object
graph = GraphFrame(vertices,edges)

#use labelpropogation to get all the communities within the graph just made and group them by their keys
#Sort the result the basis of len i.e. single node communities first
result = graph.labelPropagation(maxIter=5).select('id', 'label').rdd.map(lambda x: [x[1],x[0]]).groupByKey().map(lambda x: sorted(list(x[1]))).sortBy(lambda x: x[0]).sortBy(len).collect()
print(len(result))

#Dump it
with open(output_file,"w") as f:
    for i in result:
        f.write("'"+"', '".join(i)+"'")
        f.write('\n')
    
        






