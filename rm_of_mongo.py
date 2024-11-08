from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['arqui2']

if 'ÁreaRecreativa_Clima' in db.list_collection_names():
	db.drop_collection('ÁreaRecreativa_Clima')