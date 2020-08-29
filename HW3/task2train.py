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


input_file  = sys.argv[1]
model_file = sys.argv[2]
stopwords_file = sys.argv[3]

myNames = []
punctuations = string.punctuation


def get_tf(words):
    #Term frequency  = occurence of word in a document/ occurence of maximum occuring word
    counts = {}
    #print("\n")
    #count frequency of each word
    for word in words:
        if word in counts.keys():
            counts[word] += 1
        else:
            counts[word] = 1
    #get maximum frequency
    maximum = max(counts.values())
    #tf will be count by max
    for word in counts.keys():
        counts[word] = float(float(counts[word])/float(maximum))
    #Get the total score as tf*idf and get thee top 200 words out of it
    #total score = count into idf
    for word in counts.keys():
        counts[word] = counts[word] * idf[word]
    #sort
    items = sorted(counts.items(), key=lambda x: x[1])
    final = map(lambda x: x[0],items)
    #take top 200 words
    result = list(final)[:200]
    return result

def remove_rare(pair):
    global vocabulary
    words = pair[1]
    common_words = []
    #If the frequency of a word is less than 0.00001 than total word,remove it
    for word in words:
        freq = vocabulary[word]
        if float(freq)/float(total_no_of_words) >= (0.000001):
            common_words.append(word)
    return (pair[0],common_words)
    

def mapwords(text):
    mapping = []
    #Get word mapping for each business id using the word mapping dictionary
    word = text.split(' ')
    for x in word:
        if x != '':
            mapping.append(words[x])
    return mapping


def add_features(blist):
    #get all the business profiles for each business and concatenate them to make a particular user profile
    res = []
    blist = list(set(blist))
    for business in blist:
        res += bup[business]
    result = list(set(res))
    return result



def processtext(text):
    #from Assignment-1
    #Remove punctuations and then remove stopwords, digit, spaces and empty strings
    #text = text.encode('utf-8')
    #print(text)
    new = ""
    new = []
    text = text.replace("\n",' ')
    for word in text.lower():
        if word in punctuations:
            text = text.replace(word,"")
            
    for word in text.split(' '):
        word = word.lower().strip()
        if word not in myNames and not word.isdigit() and word != '' and word != ' ':
            new.append(word)
    new = " ".join(new)
    new = new + ' '
    return new
    


with open(stopwords_file, 'r') as f:
    myNames = [line.strip() for line in f]

review = sc.textFile(input_file).map(lambda x: json.loads(x))
#Creating the user profile
#User profiles contains all the distinct user id and business id aggregated
usp = review.map(lambda x: (x["user_id"],x["business_id"])).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y : x + y)


#Step-1. Get all the text for all the businesses and process them by removing punctuations and stopwords. Group all the business text together
data  = review.map(lambda x : (x['business_id'],x['text'])).map(lambda x: (x[0],processtext(x[1]))).reduceByKey(lambda x,y: x+y)

#Count total no of businesses
total = data.count()

#Step-2. Get all the distinct words from all the reviews text
distinct_words = data.map(lambda x: (x[0],x[1].split(' '))).flatMap(lambda x : x[1]).filter(lambda x : x!='').distinct().collect()

#Give index to each word
words = {}
count = 0
for x in distinct_words:
    words[x] =count
    count = count + 1
    
#Map business words
mapped = data.map(lambda x:( x[0],mapwords(x[1])))
#print(mapped.collect())

#Get all the words and its count in ordered form
vocabulary = mapped.flatMap(lambda x: x[1]).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).collectAsMap()

#Get total no of words
total_no_of_words = sum(vocabulary.values())

#Remove rare words
new_vocab = mapped.map(remove_rare)

#Now calculate IDF
#IDF = Total no of documents/ no of doc with term x
idf = new_vocab.map(lambda x: (x[0],set(x[1]))).flatMap(lambda x: x[1]).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).mapValues(lambda x: math.log(total/x,2)).collectAsMap()

#construct business profiles
#Business profile contains processed words for each business id
bup = new_vocab.mapValues(get_tf).collectAsMap()


#construct user profiles by appending all the words(excluding rare and stopwords) that came in business id for a particular user id
#User proofile is union of all the business profile for a user id
usp = usp.mapValues(add_features).collectAsMap()

#construct the model by dumping both profiles
model = [bup,usp]
with open(model_file, 'w') as outfile:
    json.dump(model, outfile)








