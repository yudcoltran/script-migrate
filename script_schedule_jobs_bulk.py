from pymongo import MongoClient
import re
import pprint
from bson.objectid import ObjectId

printer = pprint.PrettyPrinter()

def convertSj(data):
    res = re.findall(r'\d+', data)
    return res

#TODO:Ket noi lai db o day
client = MongoClient(
    "mongodb://localhost:27017"
)

schedule_jobs_collection = client['bulk-messaging-db'].schedule_jobs

jobs = schedule_jobs_collection.find({"job_code": {"$exists": True, "$type": 2}})

total = schedule_jobs_collection.count_documents(filter={"job_code": {"$exists": True, "$type": 2}})
printer.pprint(list(jobs))
print(total)

count = 0
for job in jobs: 
    _id = job.get('_id')
    job_code = job.get('job_code', None)
    new_job_code = convertSj(job_code)[0]
    job_code_alias = "BC" + new_job_code
    result = schedule_jobs_collection.update_one({"_id": _id}, {
        "$set": {
            "job_code": int(new_job_code),
            "job_code_alias": job_code_alias
        }
    } ).modified_count
    count+= 1
        
print(f'{count} documents take effect')

