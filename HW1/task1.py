import sys
import json
from pyspark import SparkContext, SparkConf
import pyspark
from operator import add


sc= SparkContext("local[*]")
sc.setLogLevel("ERROR")

file_path= sys.argv[1]
output_path= sys.argv[2]
stopwords = sys.argv[3]
y = sys.argv[4]
m = int(sys.argv[5])
n = int(sys.argv[6])


punctuations = ['(',')','[',']',',','.','!','?',':',';']
res = {}


with open(stopwords, 'r') as f:
    myNames = [line.strip() for line in f]
    
#print(myNames)

#print("-------------")
#TASK 1
#print("TASK1")
reviews = sc.textFile(file_path)
total_reviews = reviews.map(lambda x: json.loads(x))
total_reviews_count = total_reviews.count()
res['A'] = total_reviews_count


#TASK 2
#print("TASK2")
task2 = total_reviews.filter(lambda x: x['date'].split(' ')[0].split('-')[0] == y)
reviews_year = task2.count()
res['B'] = reviews_year
#print(reviews_year)

#TASK 3
#print("TASK3")
users = total_reviews.map(lambda x: x['user_id']).distinct()
distinct_users = users.count()
res['C'] = distinct_users
#print(distinct_users)

#TASK 4
#print("TASK4")
top_users = total_reviews.map(lambda x : (x['user_id'],1)).reduceByKey(lambda x,y : x + y).sortBy(lambda x: (-x[1],x[0])).take(m)
res['D'] = top_users

#with open(output_path,'w') as f:
#    json.dump(res , f)

        
def removepunctuation(word):
    word = word.strip()
    for char in word:
        if char in punctuations:
            word = word.replace(char, "")
    return (word.lower())


        
#TASK 5
#print("TASK5")
words = total_reviews.flatMap(lambda x : x['text'].strip().split(' '))
words = words.map(removepunctuation).filter(lambda x: x not in myNames).filter(lambda x: x!= "")
words  = words.map(lambda x : (x,1))
#print("1")
awords =  words.reduceByKey(lambda x,y : x + y)
#print("2")
sorted_word_counts = awords.sortBy(lambda x: (-x[1], x[0]))
#print("3")
top_n_freq_words = sorted_word_counts.map(lambda x: x[0])
res['E'] = top_n_freq_words.take(n)
#print(res['E'])
#print(top_n_freq_words.take(20))
#print("Done")


with open(output_path,'w') as f:
    json.dump(res , f)








    


