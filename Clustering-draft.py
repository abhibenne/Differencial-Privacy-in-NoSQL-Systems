import pymongo

import pprint


myclient = pymongo.MongoClient("mongodb+srv://admin:admin@cluster1.ajaye.mongodb.net/")
# print(myclient.list_database_names())
mydb = myclient["public"]
mycol = mydb["completeride"]
# print(mydb.list_collection_names())
# print(mydb)


# was putting the output to my localhost 
client2 = pymongo.MongoClient("localhost:27017")
mydb2 = client2["public"]
mycol2 = mydb2["relativeError"]



# total_distance is the field i tested it on, can replace with variable
# before that a simple numeric check to be added
# date functionality to be added
def cluster():
	# print(mycol.find_one())
	mean = mycol.aggregate([  
	  {
	    "$match": {
	      "total_distance": {
	        "$exists": "true"
	      }
	    }
	  },
	  {
	    "$group": {
	      "_id": "_id",
	      "average_distance": {
	        "$avg": "$total_distance"
	      }
	    }
	  }  
	]);
	
	std = st = mycol.aggregate([  
	  {
	    "$match": {
	      "total_distance": {
	        "$exists": "true"
	      }
	    }
	  },
	  {
	    "$group": {
	      "_id": "_id",
	      "std": {
	        "$stdDevPop": "$total_distance"
	      }
	    }
	  }  
	]);
	
	mean_val=0
	std_val=0
	for i in mean:
		mean_val = i['average_distance']
		print(mean_val)
	for i in std:
		std_val = i['std']
		print(i)
	
	for i in mycol2.find():
		print(i)
	
	'''
	# RELATIVE ERROR 
	
	for post in mycol.find():
		relative_error = (float(mean_val - post['total_distance'])/std_val)
		tdict = {}
		tdict['total_distance'] = post['total_distance']
		tdict['relative_error'] = relative_error
		# print(relative_error)
		# print(tdict)
		x=mycol2.insert_one(tdict)
		# break;
		# pprint.pprint(post['destination'])
		# break
	'''

# def shuffle():
	



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