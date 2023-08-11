from pymongo import MongoClient
from pymongo.collation import Collation, CollationStrength
import pprint
from bson.objectid import ObjectId
import time

printer = pprint.PrettyPrinter()

#TODO: Change mongodb URI here
client = MongoClient("mongodb://root:vinbdi%402022%40%23@mongodb.chatbot:27017/")


quick_collection = client['ConversationDB'].QuickReplies
data_duplicate = quick_collection.aggregate(

    pipeline=[

        {"$project": {"_id": 1, 
                    "code": 1,
                    "agent_id": 1}},

        {"$group": {'_id': {

            "code": "$code",
            
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
#Find duplicate docmuments
quick_rep_list = []
count = 0
for item in data_duplicate: 
    count =+ 1
    quick_rep_list.extend(item.get('data', None))
    printer.pprint(item)
    
print(f"{count} duplicate cases")
printer.pprint(quick_rep_list)

#delete duplicate documents
# deleted = 0
# for id in quick_rep_list:
#     _id = id.get('id', None)
#     quick_collection.delete_one({"_id": _id})
#     deleted += 1

# print(f"{deleted} documents were deleted")