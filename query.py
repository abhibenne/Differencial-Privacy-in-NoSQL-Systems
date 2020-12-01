import sys
import pymongo
import pprint
import json
import pandas as pd
import numpy as np
from scipy import stats
import math
import time
import matplotlib.pyplot as plt
plt.style.use('seaborn-whitegrid')

def query_aggregate_check(query):
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

def gs_sum(df, u, epsilon):
    df_clipped = df.clip(upper=u)
    
    noisy_sum = laplace_mech(df_clipped.sum(), u, .5*epsilon)
   
    # print(noisy_sum)
    # print(' is from gsum')
    return noisy_sum 

def f_sum(df):
    return df.sum()

def saa_sum(df,k, epsilon,l,u, logging=False):
    # df = adult['Daily Mean Travel Time (Seconds)']
    
    # Calculate the number of rows in each chunk
    chunk_size = int(np.ceil(df.shape[0] / k))
    
    if logging:
        print(f'Chunk size: {chunk_size}')
        
    # Step 1: split `df` into chunks
    xs      = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
    
    # Step 2: run f on each x_i and clip its output
    answers = [f_sum(x_i) for x_i in xs]
    
    
   
    clipped_answers = np.clip(answers, l, u)
    
    # Step 3: take the noisy mean of the clipped answers
    noisy_mean = laplace_mech(np.sum(clipped_answers), (u-l)/k, epsilon)
    return noisy_mean

def f_mean(df):
    return df.mean()

def saa_avg_age(df,k, epsilon,l,u, logging=False):
    # df = adult['Daily Mean Travel Time (Seconds)']
    
    # Calculate the number of rows in each chunk
    chunk_size = int(np.ceil(df.shape[0] / k))
    
    if logging:
        print(f'Chunk size: {chunk_size}')
        
    # Step 1: split `df` into chunks
    xs      = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
    
    # Step 2: run f on each x_i and clip its output
    answers = [f_mean(x_i) for x_i in xs]
    
   
    clipped_answers = np.clip(answers, l, u)
    
    # Step 3: take the noisy mean of the clipped answers
    noisy_mean = laplace_mech(np.mean(clipped_answers), (u-l)/k, epsilon)
    return noisy_mean

def ptr_avg(df, u, b, epsilon, delta, logging=False):
    df_clipped = df.clip(upper=u)
    k = dist_to_high_ls(df_clipped, u, b)

    noisy_distance = laplace_mech(k, 1, epsilon)
    threshold = np.log(2/delta)/(2*epsilon)

    if logging:
        print(f"Noisy distance is {noisy_distance} and threshold is {threshold}")

    if noisy_distance <= threshold:
        return laplace_mech(df_clipped.mean(), b, epsilon)
    else:
        return None

def gs_avg(df, u, epsilon):
    df_clipped = df.clip(upper=u)
    
    noisy_sum = laplace_mech(df_clipped.sum(), u, .5*epsilon)
    noisy_count = laplace_mech(len(df_clipped), 1, .5*epsilon)
    
    return noisy_sum / noisy_count

def gs_count(df, u, epsilon):
    df_clipped = df.clip(upper=u)
    
    
    noisy_count = laplace_mech(len(df_clipped), 1, .5*epsilon)
    
    return noisy_count


def f_count(df):
    return df.count()

def saa_count(df, k, epsilon,l,u, logging=False):
    # df = adult['Daily Mean Travel Time (Seconds)']
    
    # Calculate the number of rows in each chunk
    chunk_size = int(np.ceil(df.shape[0] / k))
    
    if logging:
        print(f'Chunk size: {chunk_size}')
        
    # Step 1: split `df` into chunks
    xs      = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
    
    # Step 2: run f on each x_i and clip its output
    answers = [f_count(x_i) for x_i in xs]
    
    
   
    clipped_answers = np.clip(answers, l, u)
    
    # Step 3: take the noisy mean of the clipped answers
    noisy_mean = laplace_mech(np.sum(clipped_answers), (u-l)/k, epsilon)
    return noisy_mean

def f_min(df):
    return df.min()

def saa_min(df, k, epsilon,l,u, logging=False):
    # df = adult['Daily Mean Travel Time (Seconds)']
    
    # Calculate the number of rows in each chunk
    chunk_size = int(np.ceil(df.shape[0] / k))
    
    if logging:
        print(f'Chunk size: {chunk_size}')
        
    # Step 1: split `df` into chunks
    xs      = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
    
    # Step 2: run f on each x_i and clip its output
    answers = [f_min(x_i) for x_i in xs]
    
    
   
    clipped_answers = np.clip(answers, l, u)
    
    # Step 3: take the noisy mean of the clipped answers
    noisy_mean = laplace_mech(min(clipped_answers), (u-l)/k, epsilon)
    return noisy_mean

def f_max(df):
    return df.max()
def saa_max(df, k, epsilon,l,u, logging=False):
    # df = adult['Daily Mean Travel Time (Seconds)']
    
    # Calculate the number of rows in each chunk
    chunk_size = int(np.ceil(df.shape[0] / k))
    
    if logging:
        print(f'Chunk size: {chunk_size}')
        
    # Step 1: split `df` into chunks
    xs      = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
    
    # Step 2: run f on each x_i and clip its output
    answers = [f_max(x_i) for x_i in xs]
    
    
   
    clipped_answers = np.clip(answers, l, u)
    
    # Step 3: take the noisy mean of the clipped answers
    noisy_mean = laplace_mech(max(clipped_answers), (u-l)/k, epsilon)
    return noisy_mean


def query_execute(df,colval):

	# print(df.head())
	# return
	dfcol = df[colval]
	dfcol = dfcol.dropna(how='all')


	# dfcol = dfcol[dfcol.notna()]
	# if(len(dfcol) == 0):
	# 	return

	# print(dfcol.head())
	# dfcol.sort_values(ascending=True)
	
	# print(dfcol.head())
	z_scores = stats.zscore(dfcol)
	
	abs_z_scores = np.abs(z_scores)
	filtered_entries = (abs_z_scores < 3)
	dfcol2 = dfcol[filtered_entries]

	if len(dfcol2)==0:
		print('Single value so not given')
		return 
	x= math.ceil(dfcol2.max())
	
	plt.plot([laplace_mech(dfcol2.clip(lower=0, upper=i).sum(), i, epsilon_i) for i in range(x)]);
	plt.show()
	plt.plot([laplace_mech(dfcol2.clip(lower=0, upper=2**i).sum(), 2**i, epsilon_i) for i in range(15)]);
	plt.show()
	# print(x)
	w = [laplace_mech(dfcol2.clip(lower=0, upper=2**i).sum(), 2**i, epsilon_i) for i in range(15)]#[laplace_mech(dfcol2.clip(lower=0, upper=i).sum(), i, epsilon_i) for i in range(x)]
	
	# w = [laplace_mech(dfcol2.clip(lower=0, upper=i).sum(), i, epsilon_i) for i in range(x)]
	
	
	# mini = 1000000
	# index = 0
	# b = [0]*len(w)
	# b[0] = w[0]
	value=0
	for i in range(1,len(w)-1):
		if w[i]<w[i-1]:
			value=i
			break
	    # b[i] = w[i]-w[i-1]
	# value = 0
	# mini = 100000
	# for i in range(len(w)-1,0):
	# 	if(b[i]<mini):
	# 		mini = b[i]
	# 		value = i
	upper = 2**value #value
	
	# for i in b:
	# 	print(i)
	
	# print(str(upper)+' is the upper')
	
	
	if aggrfunc == 'sum':
		b1 = time.time()
		summ = dfcol.sum()
		b2 = time.time()
		# print(str(dfcol.sum())+' is the original value of sum')
		# gsum method 
		epsilon = 1 
		gval = gs_sum(dfcol, upper, epsilon)
		# print(gval)
		b3 = time.time()
		#saa
		sval = saa_sum(dfcol,60, 1, 0,upper,logging=False)
		b4 = time.time()
		print('original output:'+str(summ)+' Time taken: '+str(b2-b1))
		print('GS output: '+str(gval)+' Time taken: '+str(b3-b2))
		print('SAA output: '+str(sval)+' Time taken: '+str(b4-b3))
		
		# print(val)
	elif aggrfunc == 'avg':
		b1 = time.time()
		mean = dfcol.mean()
		# print(str(dfcol.mean())+' is the original value')
		b2 = time.time()
		avgval = saa_avg_age(dfcol, 60, 1, 0,upper,logging=False)
		b3 = time.time()
		# print(avgval)
		epsilon = 1                # set epsilon = 1
		delta = 1/(len(df)**2)     # set delta = 1/n^2
		b = 0.005                  # propose a sensitivity of 0.005
		avgval2 = ptr_avg(dfcol, upper, b, epsilon, delta, logging=False)
		b4 = time.time()
		# print(avgval2)
		avgval3  = gs_avg(dfcol, upper, epsilon)
		b5 = time.time()
		# print(avgval3)
		print('original output:'+str(mean)+' Time taken: '+str(b2-b1))
		print('Saa output: '+str(avgval)+' Time taken: '+str(b3-b2))
		print('Ptr output: '+str(avgval2)+' Time taken: '+str(b4-b3))
		print('GS output: '+str(avgval3)+' Time taken: '+str(b5-b4))
	elif aggrfunc == 'count':
		b1 = time.time()
		length = len(dfcol)
		b2 = time.time()
		# print(str(len(dfcol))+' is the original value')
		epsilon=1
		cval = gs_count(dfcol, upper, epsilon)
		b3 = time.time()
		# print(cval)
		cval2 = saa_count(dfcol, 60, 1, 0,upper,logging=False)
		b4 = time.time()
		# print(cval2)
		print('original output:'+str(length)+' Time taken: '+str(b2-b1))
		print('GS output: '+str(cval)+' Time taken: '+str(b3-b2))
		print('SAA output: '+str(cval2)+' Time taken: '+str(b4-b3))
	elif aggrfunc == 'min':
		b1 = time.time()
		mini = dfcol.min()
		# print(str(dfcol.min())+' is the original value')
		b2 = time.time()
		mval = saa_min(dfcol,60, 1, 0,upper,logging=False)
		b3 = time.time()
		print('original output:'+str(mini)+' Time taken: '+str(b2-b1))
		print('SAA output: '+str(mval)+' Time taken: '+str(b3-b2))
		# print(mval)
	elif aggrfunc == 'max':
		b1 = time.time()
		maxi = dfcol.max()
		# print(str(dfcol.max())+' is the original value')
		b2 = time.time()
		mval = saa_max(dfcol,60, 1, 0,upper,logging=False)
		b3 = time.time()
		# print(mval)
		print('original output:'+str(maxi)+' Time taken: '+str(b2-b1))
		print('SAA output: '+str(mval)+' Time taken: '+str(b3-b2))
		

myclient = pymongo.MongoClient("mongodb+srv://admin:admin@cluster1.ajaye.mongodb.net/")

mydb = myclient["public"]
mycol = mydb["completeride"]

# was putting the output to my localhost 
client2 = pymongo.MongoClient("localhost:27017")
mydb2 = client2["public"]
mycol2 = mydb2["clufferedCollection"]



# print(mydb.list_collection_names())

# client2 = pymongo.MongoClient("localhost:27017")
# mydb2 = client2["public"]
# mycol2 = mydb2["example"]


# Simple example
# mylist = [{'product': "toothbrush", 'total': 4.75, 'customer': "Mike"},
# {'product': "guitar", 'total': 199.99, 'customer': "Tom"},
# {'product': "milk", 'total': 11.33, 'customer': "Mike"},
# {'product': "pizza", 'total': 8.50, 'customer': "Karen"},
# {'product': "toothbrush", 'total': 4.75, 'customer': "Karen"},
# {'product': "pizza", 'total': 4.75, 'customer': "Dave"},
# {'product': "toothbrush", 'total': 4.75, 'customer': "Mike"}]
# x = mycol2.insert_many(mylist)
# exit(0)

query1 = "mycol.aggregate([{'$group' : {'_id': '$price', 'total_value':{'$min' : '$total_distance'}, 'some_other_value':{'$avg': '$price' } } }])"
query2 = "mycol.find({'purchase_date': { '$gt': '2009-12-31','$lt': '2011-01-01'} }, { 'model_id': '1'});"

query3 = "mycol.aggregate([{'$group' : {'_id': ' ', 'total_value':{'$min' : '$total_distance'}, 'some_other_value':{'$avg': '$price' } } }])"

query4 =   "mycol.aggregate([{'$group' : {'_id': ' ', 'total_value':{'$avg' : '$total_distance'} } }])"

query5 =   "mycol.aggregate([{'$group' : {'_id': ' ', 'total_value':{'$sum' : '$price'} } }])"

mycol3 = mydb["car"]
mycol4 = mydb["card_"]
query6 = "mycol3.aggregate([{'$group' : {'_id': ' ', 'total_value':{'$avg' : '$curr_condition'} } }])"

query7 = "mycol.aggregate([{'$group' : {'_id': ' ', 'total_value':{'$count' : '$total_distance'} } }])"

query = "mycol.aggregate([{'$group' : {'_id':  ' ', 'total_value':{'$min' : '$total_distance'} } }])"



if query_aggregate_check(query) == False:
	print('Query is not aggregate')
	exit(0)


# cursor = mycol.find({})
# for document in cursor: 
#     print(document.keys())  # print all fields of this document. 
#     break

# for i in mycol3.find():
# 	print(i)
# 	exit(0)

df = pd.DataFrame(list(mycol.find()))


ind=query.find('$group')+1
query = query[ind:]
q = query[query.find('_id'):]

idcol = ''
if q.find(',')>q.find('$'):
	idcol = q[q.find('$')+1:q.find(',')-1]
	query = q[q.find('$')+1:]
	print(query+' is query')


while(ind<len(query) and query.find('$')!=-1):
	# query = query[ind:]
	ind = query.find('$')+1
	# print(query)
	# print(ind)
	query = query[ind:]
	aggrfunc = query[:query.find('\'')]
	ind = query.find('$')+1
	query = query[ind:]
	colval = query[:query.find('\'')]

	print(aggrfunc+' is the aggregation on '+colval+' field',sep=' ')
	# print(query)
	# continue



	if idcol=='':
		query_execute(df,colval)
		print()
	else:
		print(' and group by '+idcol)
		# print(df[idcol].unique())
		for value in df[idcol].unique():
			print(value)
			is_value = df[idcol]==value
			subdf = df[is_value]
			subdf = subdf.dropna(subset=[colval])
			# subdf = subdf[subdf[colval].notna()]
			# print(subdf.head(2))
			if (len(subdf)>0):
				query_execute(subdf,colval)
			else:
				print('Nan')









# p1 = query.find('$group')
# p2 = query.find('])')
# group = query[p1+3:p2-1]
# # print(group)
# param = group.split(':')
# # print(param)
# param2 = param[3]
# aggrfunc = param2.strip('{\'$ ')
# # print(aggrfunc)
# param3 = param[4]
# colval = param3[param3.find('$')+1:param3.find('}')-1]
# print(colval)

# uncomment and align this part at end
# try:
# collection_cursor = eval(query)
# # print(collection_cursor)
# for i in collection_cursor:
# 	print(i)
# except:
	# print('Not a valid query')







# Query 1 - 
# query = "mycol.aggregate([{'$group' : {'_id': ' ', 'total_value':{'$min' : '$total_distance'}, 'some_other_value':{'$avg': '$price' } } }])"
# min is the aggregation on total_distance field
# original output:2.11 Time taken: 0.0
# SAA output: -4.3890231759592915 Time taken: 0.00897526741027832

# avg is the aggregation on price field
# original output:36.02746 Time taken: 0.0
# Saa output: 38.23668590197573 Time taken: 0.009000062942504883
# Ptr output: 36.02767251400291 Time taken: 0.0
# GS output: 35.2192895148833 Time taken: 0.0009953975677490234




# Query 2