import sys
import pymongo
import pprint
import json
import pandas as pd
import numpy as np
from scipy import stats
import math

def query_check(query):
	pos1 = query.find('.')
	pos2 = query.find('(')
	if query[pos1+1:pos2]=='aggregate':
		return True
	return False

def laplace_mech(v, sensitivity, epsilon):
    return v + np.random.laplace(loc=0, scale=sensitivity/epsilon)
epsilon_i = 1


def ls_at_distance(df, u, k):
    return np.abs(u/(len(df) - k + 1))

def dist_to_high_ls(df, u, b):
    k = 0
    
    while ls_at_distance(df, u, k) < b:
        k += 1
    
    return k

def ptr_sum(df, u, b, epsilon, delta, logging=False):
    df_clipped = df.clip(upper=u)
    k = dist_to_high_ls(df_clipped, u, b)

    noisy_distance = laplace_mech(k, 1, epsilon)
    threshold = np.log(2/delta)/(2*epsilon)

    if logging:
        print(f"Noisy distance is {noisy_distance} and threshold is {threshold}")

    if noisy_distance <= threshold:
        return laplace_mech(df_clipped.sum(), b, epsilon)
    else:
        return None



myclient = pymongo.MongoClient("mongodb+srv://admin:admin@cluster1.ajaye.mongodb.net/")

mydb = myclient["public"]
mycol = mydb["completeride"]
# print(mydb.list_collection_names())


query = "mycol.aggregate([{'$group' : {'_id': '', 'total_distance':{'$sum' : 1} } }])"
# "mycol.find({},{'_id':2}).limit(10)" 
# sys.argv[1]


if query_check(query) == False:
	print('Query is not aggregate')
	exit(0)

# print(query.find('\'$'))
p1 = query.find('([')
p2 = query.find('])')
group = query[p1+3:p2-1]
# print(group)
param = group.split(':')[2:]
# print(param)
colval = param[0].split(',')[1]
colval = colval.strip('\' ')
# print(colval)
# print(param[1].strip('{\'$ '))
aggrfunc = param[1].strip('{\'$ ')
# print(aggrfunc)

df = pd.DataFrame(list(mycol.find()))

# print(df.head())
dfcol = df[colval]
# print(dfcol.head())

z_scores = stats.zscore(dfcol)

abs_z_scores = np.abs(z_scores)
filtered_entries = (abs_z_scores < 3)
dfcol2 = dfcol[filtered_entries]

# print(dfcol2.head())

x= math.ceil(dfcol2.max())

# print(x)
w = [laplace_mech(dfcol2.clip(lower=0, upper=i).sum(), i, epsilon_i) for i in range(x)]


mini = 1000000
index = 0;
b = [0]*len(w)
b[0] = w[0]
for i in range(1,len(w)-1):
    b[i] = w[i]-w[i-1]
value = 0
mini = 100000
for i in range(len(w)-1):
	if(b[i]<mini):
		mini = b[i]
		value = i
upper = value

print(upper)


if aggrfunc == 'sum':
	epsilon = 1              # set epsilon = 1
	delta = 1/(len(df)**2)     # set delta = 1/n^2
	b = 0.005                  # propose a sensitivity of 0.005
	
	val = ptr_sum(dfcol, upper, b, epsilon, delta, logging=True)
	print(val)

# uncomment and align this part at end
# try:
# collection_cursor = eval(query)
# print(collection_cursor)
# for i in collection_cursor:
# 	print(i)
# except:
	# print('Not a valid query')