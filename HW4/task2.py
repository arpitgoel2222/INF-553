from pyspark import SparkContext
import json
import sys
import time
from queue import *
import pyspark
import itertools
from itertools import combinations
import collections

s = pyspark.SparkConf().setAppName('HW4')
sc = SparkContext(conf = s)
sc.setLogLevel("Error")

threshold = int(sys.argv[1])
input_file = sys.argv[2]
output1 = sys.argv[3]
output2 = sys.argv[4]

def delete_component(remainder_graph, component):
#Remove vertices from the graph from the connecteed components detected
	for v in component[0]:
		del remainder_graph[v]
#After removing the vertices from the reamining hraph delete the edges from the adjacency matrix as well.
	for node in remainder_graph.keys():
		adj_list = remainder_graph[node]
		for vertex in component[0]:
			if(vertex in adj_list):
				adj_list.remove(node[1])
            #Update the remaining graph with the new adjacemcy list
		remainder_graph[node] = adj_list		

	return remainder_graph		

def find_joined_graph(adjacent_vertices):
    connected_components= []
    remainder_graph = adjacent_vertices
    #First check, whether the graph is empty or not.
    while(checkempty(remainder_graph) == False):
        vertices= []
        #Get all the distinct vertices in the given remaining graph i.e. remove duplicate vertices as well.
        for key, value in remainder_graph.items():
            vertices.append(key)
        vertices = list(set(vertices))
        #Implement bfs again through the root node
        root = vertices[0]
        visited=[]
        edges= []
        queue = Queue(maxsize = len(vertices))
        #Put current node in the queuue and traverse the graoh through its children
        queue.put(root)
        visited.append(root)
        while(queue.empty()!= True):
            node = queue.get()
            children = adjacent_vertices[node]

            for i in children:
                if(i not in visited):
                    queue.put(i)
                    visited.append(i)
            #Check all the edges that are not travresed yet and then make a list to contain all edges as well as the visited nodes
                pair = sorted((node,i))
                if(pair not in edges):
                    edges.append(pair)
        comp_g = (visited, edges)
        #Connected components will contain all those edges and vertices (traversed and not yet traversed)
        connected_components.append(comp_g)
        #Remaining graph will get updated by removing all those components that are already visited
        remainder_graph= delete_component(remainder_graph, comp_g)
    return connected_components
            
def update_matrix(connected_components):
    result = {}
    #We need to add both i.e. a-b and b-a
    #Check if there is and edge between two nodes, append the other node in the node
    for c in connected_components:
        for edge in c[1]:
            if(edge[0] in result.keys()):
                result[edge[0]].append(edge[1])
            else:
                result[edge[0]]= [edge[1]]
    #Check if there is and edge between two nodes, append the other node in the node, from other side this time
            if(edge[1] in result.keys()):
                result[edge[1]].append(edge[0])
            else:
                result[edge[1]]= [edge[0]]
    #Return the adjacency matrix
    return result

def delete_edge(adjacency_matrix, edge_to_remove):
    #Remove the highest betweenneess edge from the graph
    if(edge_to_remove[0] in adjacency_matrix.keys()):
        l = adjacency_matrix[edge_to_remove[0]]
        if(edge_to_remove[1] in l):
            l.remove(edge_to_remove[1])
    #As we have undirected graph, remove both sides of the edges , i.e. a->b and b->a
    if(edge_to_remove[1] in adjacency_matrix.keys()):
        l = adjacency_matrix[edge_to_remove[1]]
        if(edge_to_remove[0] in l):
            l.remove(edge_to_remove[0])
    #return the remaining graph to compute connected components
    return adjacency_matrix


def checkempty(av):
#this function will check if the remainder grapph is empty or not. We will proceed to find connected components only if it is not
    if len(av) !=0:
        for x in av.keys():
            alist = av[x]
            if len(alist)==0:
                pass
            else:
                return False
        return True

        
def getallbusinesses(x):
    res= []
    #Get all the combinations of the edges possible in the graph
    pairs = itertools.combinations(x[1], 2)
    for i in pairs:
        i = sorted(i)
        #Make a tuple along with the edge(which is the user id) and business id
        res.append(((i[0], i[1]), [x[0]]))
    return res


def calculate_betweenness(root, edges_list, vertices):
#Betweenness is calculated by bfs traversal of the graph from each node
#Bfs is done using queue. Put the current node in the queue
    q = Queue(maxsize = vertices)
    q.put(root)
    visited, levels, parents, weights = [], {}, {}, {}
    #Update the values of the  variables.
    weights[root] = 1
    visited.append(root)
    levels[root] = 0
    
    #Executing bfs again  and simultaneously updating the values of variables
    while not q.empty():
        #Get node in front of queue
        node = q.get()
        child = edges_list[node]
        #get all the children of the node
        for i in child:
            if (i in visited):
                if (i != root):
                    parents[i].append(node)
                    if (levels[i] - 1 == levels[node]):
                        weights[i] += weights[node]
            else:
                q.put(i)
                levels[i] = levels[node] + 1
                parents[i], weights[i] = [node], weights[node]
                visited.append(i)
                
    #Get nodes along with number of times it was visited and sort it in descending order
    #Asigning beetweeneness value for each node from top to bottom
    ov = list()
    rev_order = []
    count = -1
    for i in visited:
        count = count + 1
        ov.append((i, count))
    nVs = {}
    reverse_order = sorted(ov, key=(lambda x: x[1]), reverse=True)
    
    #Beetweenness for edges is calculated from bottom to top according to girvan newman, so reversing the whole list
    for i in reverse_order:
        nVs[i[0]] = 1
        rev_order.append(i[0])
    
    
    bVs = {}
    #Calculating the betweenness of edges. Itertate until we reach root node
    for j in range(len(rev_order)):
        if (rev_order[j] != root):
            t_w = 0
            #Adding the nod ebetweeness for each edge
            for i in parents[rev_order[j]]:
                if (levels[i] == levels[rev_order[j]] - 1):
                    t_w += weights[i]
            #Check if the node has two children , then the betweenness will be half of each,else get full credit of betweenness
            for i in parents[rev_order[j]]:
                if (levels[i] == levels[rev_order[j]] - 1):
                    src, dest = rev_order[j], i
                    if src >= dest:
                        p = (dest,src)
                        pair = tuple(p)
                    else:
                        p = (src,dest)
                        pair = tuple(p)
                    if (pair in bVs.keys()):
                        bVs[pair] += float(nVs[src] * weights[dest] / t_w)
                    else:
                        bVs[pair] = float(nVs[src] * weights[dest] / t_w)
                    #Add all the values until the root is reached
                    nVs[dest] += float(nVs[src] * weights[dest] / t_w)
    
    #gET betweeness for each edge
    bList = list()
    for k, v in bVs.items():
        bList.append([k, v])
    return bList

    
    
def getvertices(node, edges):
    av = []
    #This function get all the vertices adjacent to each other
    #Iterate each edge and check if one of tbe vertice is same,get the other one
    for edge in edges:
        if edge[0] == node:
            av.append(edge[1])
        elif edge[1] == node:
            av.append(edge[0])
    return list(set(av))

#Get the data and grup it by business id and user id
data= sc.textFile(input_file).map(lambda x : x.split(',')).filter(lambda x: x[0]!= "user_id").map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y)

#Get all the combinations of business id of a user id anf group it together
#Filter it on the basis of threshold and get the first set
users = data.flatMap(lambda x: getallbusinesses(x)).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) >= threshold).map(lambda x: x[0])

#Get the number of nodes/users present in the graph
nodes = users.flatMap(lambda x: [(x[0]),(x[1])]).distinct()
no_of_nodes = len(nodes.collect())

#Get all the edges present in the graph
edges = users.map(lambda x: (x[0], x[1]))
edge_list = edges.collect()


#Get vertices adjacent to each other
vert = nodes.map(lambda x: (x, getvertices(x, edge_list))).collectAsMap()
#print(len(vert))
#Task1
#calculate betweenness in the graph using the vertices
#Divide the betweenness by 2 as it is a directed graph and sort it accordingly after getting the pair of tuple
btw = nodes.flatMap(lambda x: calculate_betweenness(x, vert, no_of_nodes)).reduceByKey(lambda x,y: (x+y)).map(lambda x: (x[0], float(x[1]/2))).sortByKey().map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))

#Dump it
with open(output1,"w") as f:
    for pair in btw.collect():
        f.write(str(pair[0]) + ", " + str(pair[1]) + "\n")
        

#Task2
#Take the edge with highest betweenness and remove it according to girvan newman
edge_to_remove = btw.take(1)[0][0]
    
no_of_edges = len(edge_list)
adjacency_matrix = vert.copy()

#Get the remaining graph or connected parts in the graph after removing the highest betweenness edge
#Calculate the modularity for the remaining graph
# Modularity = [(edges within group s) – (expected edges within group s) ] over all total no of edges
connected_components = find_joined_graph(adjacency_matrix)

modularity= 0
for c in connected_components:
    count = 0
    for i in c[0]:
        for j in c[0]:
            count = 0;
            adj_list = vert[str(i)]
            if(j in adj_list):
                count=1
            ki = len(vert[i])
            kj = len(vert[j])
            modularity += count - ((ki*kj)/(2*no_of_edges))
modularity= modularity/(2*no_of_edges)


adjacency_matrix= vert.copy()
    
highest_modularity = -1
communities=[]
count=0
#Iteratively repeat the whole process until a termination condition
    
while(1):
    #Remove an edge from the graph and get all the connected parts in the graph
    #Calculate the modularity for the remaining graph
    adjacency_matrix = delete_edge(adjacency_matrix, edge_to_remove)
    connected_components = find_joined_graph(adjacency_matrix)
    # Modularity = [(edges within group s) – (expected edges within group s) ] over all total no of edges
    for c in connected_components:
        counter = 0
        for i in c[0]:
            for j in c[0]:
                counter = 0;
                adj_list = vert[str(i)]
                if(j in adj_list):
                    counter=1
                
                ki = len(vert[i])
                kj = len(vert[j])
                modularity += counter - ((ki*kj)/(2*no_of_edges))
    modularity= modularity/(2*no_of_edges)

    #After removing the edge, build the adjacency matrix for the graph from the connected components
    adjacency_matrix= update_matrix(connected_components)
    
    #Remove duplicates
    temp=[]
    for i in adjacency_matrix.keys():
        temp.append(i)
    temp = list(set(temp))
        
        
    #Calculate the betweenness value for each edge in the graph by converting the vertices in the graph into a rdd
    vert_rdd = sc.parallelize(temp)
    betweenness_temp = vert_rdd.flatMap(lambda x: calculate_betweenness(x, adjacency_matrix, no_of_nodes)).reduceByKey(lambda x,y: (x+y)).map(lambda x: (x[0], float(x[1]/2))).sortByKey().map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))
    
    #Remove the highest betweenness edge
    edge_to_remove = betweenness_temp.take(1)[0][0]
    
    #Iteratively check the condition for the updated connected components graph and update it and get the communities
    if(modularity >= highest_modularity):
        highest_modularity = modularity;communities = connected_components
    
    #Termination condition
    count += 1
    if(count == 75):
        break

sorted_communities= []
#Sort the communities based on their length and then sort it lexiographically and then get the second part
#print(len(communities))
for community in communities :
    item = sorted(community[0])
    sorted_communities.append((item,len(item)))

sorted_communities.sort()
sorted_communities.sort(key = lambda x:x[1])


#Dump the second task into the file
f = open(output2, 'w')
i = 0
for x in sorted_communities:
    if (i != 0):
        f.write("\n")
    else:
        i = 1
    community = str(x[0]).replace("[","").replace("]","")
    f.write(community)
    



