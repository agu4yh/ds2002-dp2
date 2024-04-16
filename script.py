from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.agu4yh
# specify a collection
collection = db.data_files

path = "data"
# variable used to track the number of imported files
imported = 0

# for each file in the data folder
for (root, dirs, files) in os.walk(path):
    for f in files:
        filepath = os.path.join(root, f)
        
        # open the file and load in the data
        with open(filepath, 'r') as file:
            try:
                file_data = json.load(file)
            # if the file is corrupted, skip it and move on to the next one
            except json.JSONDecodeError as e:
                print(f"The file {filepath} is corrupted: {e}")
                continue  
            
            # insert each record in the file into the collection 
            if isinstance(file_data, list):
                result = collection.insert_many(file_data)
                imported += len(result.inserted_ids)
            else:
                collection.insert_one(file_data)
                imported += 1

print(f"{imported} complete documents have been imported into your collection")