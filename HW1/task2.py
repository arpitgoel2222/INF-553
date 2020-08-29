from pyspark import SparkContext
import json
import sys

sc= SparkContext("local[*]")
sc.setLogLevel("ERROR")

res = {}

output_file = sys.argv[3]
option = sys.argv[4]
n = int(sys.argv[5])


if option == "spark":
    #read both files
    reviews = sc.textFile(sys.argv[1])
    
    business = sc.textFile(sys.argv[2])
    #filter business with id and category only and then split each of it after checking for its emptiness
    business = business.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], x['categories']))
    business = business.filter(lambda kv: (kv[1] is not None) and (kv[1] is not "")).mapValues(lambda values: [value.strip() for value in values.split(',')])
    
    #filter review with business and stars and then group all the stars with business
    reviews = reviews.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],x['stars']))
    reviews = reviews.groupByKey()
    
    #get sum and length of stars for each business
    reviews = reviews.map(lambda x: (x[0], (sum(x[1]), len(x[1]))))
    print(reviews.take(2))
    
    #joining the both rdd gives us categories along wiht stars for each business
    new_rdd = business.join(reviews)
    print(new_rdd.take(2))
    
    
    #new_rdd2 = business.cartesian(reviews).filter( lambda (k, v): k[0]==v[0] ).map( lambda (k, v): (k[0], (k[1], v[1])) )


    new_rdd = new_rdd.map(lambda x: x[1]).filter(lambda x: x[1] is not None)
    
    #Now the main step is to categorize each category with its stars from all businesses and then get average
    #0 is buisness id and 1 is tuple with total stars with its length
    new_rdd = new_rdd.flatMap(lambda x: [(category, x[1]) for category in x[0]])
    
    
    new_rdd = new_rdd.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda value: float(float(value[0]) / float(value[1])))
    #final step
    new_rdd =  new_rdd.sortBy(lambda x: (-x[1], x[0])).take(n)
    
    res["result"] = new_rdd
    with open(output_file, 'w') as f:
        json.dump(res, f)
        
elif option == "no_spark":
    #read both files
    reviews = []
    with open(sys.argv[1]) as fp:
        for line in fp:
            reviews.append(json.loads(line))
    
    business = []
    with open(sys.argv[2]) as fp:
        for line in fp:
            business.append(json.loads(line))
            
    print("A")
    #get category for each business
    cd={}
    for item in business:
        if (item['categories'] is not None) and (item['categories'] is not ""):
            cd[item['business_id']] = [category.strip()
                                                  for category in item['categories'].split(',')]

            
            
    print("B")
    bd ={}
    #group and summ all the stars for each business as done in spark above with dictionary
    for bus in reviews:
        if bus['business_id'] not in bd.keys():
            bd[bus['business_id']] = (float(bus['stars']),1)
        elif(2!=2):
            msg = "lets"
            msg2 = "task2"
            msg3 = msg + msg2
        else:
            addnumber = bd.get(bus['business_id'])[1] + 1
            addscore = bd.get(bus['business_id'])[0] + bus['stars']
            bd.update({bus['business_id']: (addscore,addnumber)})
            
   
    print("C")
    #join both and group category with stars for each business
    final_dict = {}
    for k,v in bd.items():
        if None is not cd.get(k):
            for category in cd.get(k):
                if category in final_dict.keys():
                    new_value = final_dict.get(category)[0] + v[0]
                    new_count = final_dict.get(category)[1] + v[1]
                    final_dict.update({category: (new_value, new_count)})
                else:
                    final_dict[category] = v
                
                    
    print("d")
    #get average
    for k,v in final_dict.items():
        final_dict.update({k:(float(v[0]/v[1]))})
    
    print("e")
    #final step
    final_dict = sorted(final_dict.items(), key = lambda x: (-x[1], x[0]))
    res["result"] = final_dict[:n]
    print(res)
    with open(output_file, 'w') as f:
        json.dump(res, f)
    
    
            

    

   
 

    
    
    
    
    

