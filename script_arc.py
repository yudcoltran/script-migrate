from pymongo import MongoClient
from pymongo.collation import Collation, CollationStrength
import pprint
from bson.objectid import ObjectId
import time

#TODO: Change db URI here
client = MongoClient("mongodb://localhost:27017")

arc_collection = client['AgentDB'].CommentConfigModel
arcs = arc_collection.aggregate(

    pipeline=[

        {"$project": {"_id": 1, 
                    "name": 1,
                    "agent_id": 1}},

        {"$group": {'_id': {

            "name": "$name",
            
            "agent_id": "$agent_id",
            
        }, 'count': {'$sum': 1}, 'data': {'$push': {'id':'$_id'}}}},

        {"$sort": {"_id": 1}},

        {'$match': {'count': {'$gt': 1}}},

        {"$project": {"data": 1}}

    ],
    allowDiskUse=True,
    collation=Collation(locale='vi',
                        strength=CollationStrength.SECONDARY)

)

printer = pprint.PrettyPrinter()
arc_list = []
count = 0

#Find duplicate docmuments
for item in arcs:
    count += 1
    arc_list.extend(item.get('data', None))
    printer.pprint(item)

print(f"{count} duplicate cases")
    
printer.pprint(arc_list)

#delete duplicate documents
# deleted = 0
# for id in arc_list:
#     _id = id.get('id', None)
#     arc_collection.delete_one({"_id": _id})
#     deleted += 1

# print(f"{deleted} documents were deleted")

