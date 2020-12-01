import pymongo

import pprint


myclient = pymongo.MongoClient("mongodb+srv://admin:admin@cluster1.ajaye.mongodb.net/")
# print(myclient.list_database_names())
mydb = myclient["public"]
mycol = mydb["completeride"]
print(mydb.list_collection_names())
#print(mydb)


# was putting the output to my localhost 
client2 = pymongo.MongoClient("localhost:27017")
mydb2 = client2["public"]
mycol2 = mydb2["relativeError"]



# total_distance is the field i tested it on, can replace with variable
# before that a simple numeric check to be added
# date functionality to be added
def cluster(colname):
	# print(mycol.find_one())
	mycol2.delete_many( { } );
	mean = mycol.aggregate([  
	  {
	    "$match": {
	      colname: {
	        "$exists": "true"
	      }
	    }
	  },
	  {
	    "$group": {
	      "_id": "_id",
	      "average_value": {
	        "$avg": "$"+colname
	      }
	    }
	  }  
	]);
	
	std = st = mycol.aggregate([  
	  {
	    "$match": {
	      colname: {
	        "$exists": "true"
	      }
	    }
	  },
	  {
	    "$group": {
	      "_id": "_id",
	      "std": {
	        "$stdDevPop": "$"+colname
	      }
	    }
	  }  
	]);
	
	mean_val=0
	std_val=0
	for i in mean:
		mean_val = i['average_value']
		print(mean_val)
	for i in std:
		std_val = i['std']
		print(i)
	
	for i in mycol2.find():
		print(i)
	
	
	# RELATIVE ERROR 
	
	for post in mycol.find():
		relative_error = (float(mean_val - post[colname])/std_val)
		tdict = {}
		tdict[colname] = post[colname]
		tdict['relative_error'] = relative_error
		x=mycol2.insert_one(tdict)
	
import pandas as pd
import random
import json

mycol3 = mydb2["alteredRelativeError"]
mycol4 = mydb2["clufferedCollection"]


def shuffle(colname):
	mycol4.delete_many( { } );
	df2 = pd.DataFrame(list(mycol2.find()))
	# print(df2.head())
	list_index=[]
	length=len(df2)
	for index in range (length):
	    list_index.append(index)
	s1 = pd.Series(list_index, name='index')
	df3 = pd.concat([s1,df2], axis=1)
	for i, trial in df3.iterrows():
		df3.loc[i, "relative_error"] = round(df3.loc[i, "relative_error"],1)
	df4=df3.groupby(['relative_error'])[colname].apply(list).reset_index(name='values')
	shuffle=df4["values"]
	for i in range(len(shuffle)):
		random.shuffle(shuffle[i])
	df5=df3.groupby(['relative_error'])['index'].apply(list).reset_index(name='index_values')
	first=df5['index_values']
	ld=pd.DataFrame(first, columns =["index_values"])
	ld1=pd.concat([df4,ld], axis=1, ignore_index=True)
	ld1.columns=["relative_error",colname,"index_values"]
	check=ld1["index_values"]
	group_by_values=ld1["relative_error"]
	travel_distance=ld1[colname]
    
	list_index_values=[]
	list_group_by_values=[]
	travel_distance_values=[]
	### for index_values
	for i in range(len(ld1)):
	    for j in range(len(check[i])):
	        list_index_values.append(check[i][j])
	### for group by values
	for x in range(len(ld1)):
	    for y in range(len(check[x])):
	        list_group_by_values.append(group_by_values[x])
	### for travel distance
	for a in range(len(ld1)):
	    for b in range(len(check[a])):
	        travel_distance_values.append(travel_distance[a][b])
	shuffled_dataframe = pd.DataFrame({'index': list_index_values,colname: travel_distance_values,'relative_error': list_group_by_values})
	final = shuffled_dataframe.sort_values(by='index', ascending=True)
	dl=final.drop(columns=["index"], axis=1)
	listid = df3['_id']
	res = dl.assign(_id = listid) 

	values = dl["total_distance"].values
	ind=0
	for i in mycol.find():
		# print(i[colname])
		tdict = i.copy()
		# print(values[ind])
		tdict[colname] = values[ind]
		ind+=1
		x=mycol4.insert_one(tdict)
		# print(tdict[colname])
		# print(i[colname])
		# break



	# records = json.loads(dl.T.to_json()).values()
	# mycol3.insert_many(records)




	# TESTING WHAT IS INSERTED
	# for i in mycol3.find():
	# 	print(i);break
	# mydb2.myCollection.insert(records)


colname = "total_distance"

cluster(colname)
shuffle(colname)



# Checking basic commands

# print(st[0])

# db.posts.insert({
# 	title: 'Post one',
# 	date: Date(),
# 	likes:4,
# 	tags:['A','B']
# 	})
# 	

# db.posts.update({title:'Post Two'},{
# 	$set: {
# 		category:'Technology'
# 	}
# 	})


# db.posts.update({title:'Post one'},{
# 	$inc: {
# 		likes:2
# 	}
# 	})

'''
import psycopg2
import sys, os
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import random
from random import randrange
DATABASES = {
    'uber':{
        'NAME': 'uber_cluf',
        'USER': 'postgres',
        'PASSWORD': 'password',
        'HOST': '127.0.0.1',
        'PORT': 5432,
    },
}

db = DATABASES['uber']

engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
    user = db['USER'],
    password = db['PASSWORD'],
    host = db['HOST'],
    port = db['PORT'],
    database = db['NAME'],
)
engine = create_engine(engine_string)

def cluster_totdist():
    df_c = pd.read_sql_table(Table_name,engine)
    ld_c=df_c
    first = df_c[Column_name].to_list()
    firsta = df_c[Column_name].to_list()
    list_relative=[]
    dis=df_c[Column_name]
    time=0
    if 'datetime'in str(dis.dtype) :
        dis=pd.to_numeric(dis)
        first=dis.to_list()
        time=1
    #print(dis)
    length=len(df_c.index)
    mean_=dis.mean()
    std_=dis.std()
    i=0
    for i in range (length):
        relative_error=(float(mean_-first[i])/std_)
        if length <= 500000 and time==0 :
            relative_error=round(relative_error,0)
        elif length <= 1000000 and time==0 :
            relative_error=round(relative_error,1)
        else:
            relative_error=round(relative_error,1)
        list_relative.append(relative_error)

    surge_relative=pd.DataFrame(np.column_stack([firsta,list_relative]),columns=[Column_name,'relative_error'])
    surge_relative.to_csv("relative_cluster_"+Column_name+".csv",index=False)



def shuffle_totdist():
    actual = pd.read_sql_table(Table_name,engine)
    df = pd.read_csv("relative_cluster_"+Column_name+".csv")
    list_glob=[]
    list_index=[]
    length=len(actual.index)

    for index in range (length):
        list_index.append(index)
    s1 = pd.Series(list_index, name='index')
    df1 = pd.concat([s1,df], axis=1)

    df3=df1.groupby(['relative_error'])[Column_name].apply(list).reset_index(name='values')
    shuffle=df3["values"]
    for i in range(len(shuffle)):
        random.shuffle(shuffle[i])

    df2=df1.groupby(['relative_error'])['index'].apply(list).reset_index(name='index_values')
    first=df2['index_values']
    ld=pd.DataFrame(first, columns =["index_values"])
    ld1=pd.concat([df3,ld], axis=1, ignore_index=True)
    ld1.columns=["relative_error",Column_name,"index_values"]
    check=ld1["index_values"]
    group_by_values=ld1["relative_error"]
    travel_distance=ld1[Column_name]
    list_index_values=[]
    list_group_by_values=[]
    travel_distance_values=[]
    ### for index_values
    for i in range(len(ld1)):
        for j in range(len(check[i])):
            list_index_values.append(check[i][j])
    ### for group by values
    for x in range(len(ld1)):
        for y in range(len(check[x])):
            list_group_by_values.append(group_by_values[x])
    ### for travel distance
    for a in range(len(ld1)):
        for b in range(len(check[a])):
            travel_distance_values.append(travel_distance[a][b])

    shuffled_dataframe = pd.DataFrame(
    {'index': list_index_values,
     Column_name: travel_distance_values,
     'relative_error': list_group_by_values

    })
    final = shuffled_dataframe.sort_values(by='index', ascending=True)
    dl=final.drop(columns=["index"], axis=1)
    if 'time' in Column_name:
        dl=pd.to_datetime(dl[Column_name])
        actual[Column_name]=dl.values
    else:
        actual[Column_name]=dl[Column_name].values
    dl.to_csv("completeride_relative_cluffered"+Column_name+".csv",index=False)
    actual.to_sql(Table_name, engine, if_exists="replace",index=False)


def drop_column():
    for j in range(i):
        column_name=str(Column_name[j])
        engine.execute("ALter table "+Table_name+" Drop column "+column_name)

count=input("Enter 1 to cluffer another column, 2 to drop and 0 to exit: ")
while(count=='1'):
    Table_name=input("Enter table name: ")
    Column_name=input("Enter column name you want to apply cluffering: ")
    cluster_totdist()
    shuffle_totdist()
    count=input("Enter 1 to cluffer another column, 2 to drop and 0 to exit: ")

while(count=='2'):
    Table_name=input("Enter table name: ")
    i=int(input("Enter number of columns to drop :"))
    Column_name=[]
    for j in range(i):
        x=input("Enter column name you want to drop: ")
        Column_name.append(x)
    drop_column()
    count=input("Enter 2 to drop and 0 to exit: ")
'''