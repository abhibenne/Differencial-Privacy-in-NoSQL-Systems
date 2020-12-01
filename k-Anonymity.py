import pymongo
import pandas as pd
import pprint
import random


myclient = pymongo.MongoClient("mongodb+srv://admin:admin@cluster1.ajaye.mongodb.net/")
# print(myclient.list_database_names())
mydb = myclient["public"]
mycol = mydb["completeride"]

# print(mydb.list_collection_names())


def isKAnonymized(df, k):
    for index, row in df.iterrows():
        query = ' & '.join([f'{col} == {row[col]}' for col in df.columns])
        rows = df.query(query)
        if (rows.shape[0] < k):
            return False
    return True

def generalize(df, depths):
    return df.apply(lambda x: x.apply(lambda y: int(int(y/(10**depths[x.name]))*(10**depths[x.name]))))



def kAnonymity():
	df = pd.DataFrame(list(mycol.find()))
	df1 = df[['_id','total_distance']].copy()
	df3 = df1.drop(['_id'],axis=1)
	depths = { 'total_distance': 1 }
	df4 = generalize(df3, depths)
	df4['generalized_distance'] = df4['total_distance']
	df4['total_distance'] = df1['total_distance']
	df4['_id'] = df1['_id']
	list_index=[]
	length=len(df4)
	for index in range (length):
	    list_index.append(index)
	s1 = pd.Series(list_index, name='index')
	df4 = pd.concat([s1,df4], axis=1)
	df5=df4.groupby(['generalized_distance'])['total_distance'].apply(list).reset_index(name='values')
	df7=df4.groupby(['generalized_distance'])['index'].apply(list).reset_index(name='index_values')
	df5['index_values'] = df7['index_values']
	df6=df4.groupby(['generalized_distance'])['_id'].apply(list).reset_index(name='id_values')
	df5['id_values'] = df6['id_values']
	shuffle = df5['values'].values
	for i in range(0,len(shuffle)):
		random.shuffle(shuffle[i])
	check=df5["index_values"]
	group_by_values=df5["generalized_distance"]
	travel_distance=df5["values"]
	ids_by_values=df5["id_values"]

	list_index_values=[]
	list_group_by_values=[]
	list_group_ids = []
	travel_distance_values=[]
	### for index_values
	for i in range(len(df5)):
	    for j in range(len(check[i])):
	        list_index_values.append(check[i][j])
	### for group by values
	for x in range(len(df5)):
	    for y in range(len(check[x])):
	        list_group_by_values.append(group_by_values[x])
	### for group by id values
	for x in range(len(df5)):
	    for y in range(len(check[x])):
	        list_group_ids.append(ids_by_values[x][y])
	### for travel distance
	for a in range(len(df5)):
	    for b in range(len(check[a])):
	        travel_distance_values.append(travel_distance[a][b])
	shuffled_dataframe = pd.DataFrame({'id': list_group_ids,
     'total_distance': travel_distance_values,
     'generalized_distance': list_group_by_values,
     'index':list_index_values
    })
	final = shuffled_dataframe.sort_values(by='index', ascending=True)
	final = final.drop(['index'],axis=1)
	final = final.drop(['generalized_distance'],axis=1)
	print(final.head())

kAnonymity()