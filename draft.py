# Requires pymongo 3.6.0+
from bson.son import SON
from pymongo import MongoClient

client = MongoClient("mongodb://host:port/")
database = client["admin"]
collection = database["report_data.events"]

# Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

pipeline = [
    {
        u"$match": {
            u"type": u"contact",
            u"subtype": {
                u"$nin": [
                    u"notification"
                ]
            }
        }
    }, 
    {
        u"$sort": SON([ (u"ts", -1) ])
    }, 
    {
        u"$addFields": {
            u"ts_to": {
                u"$add": [
                    u"$ts",
                    {
                        u"$multiply": [
                            10.0,
                            60.0,
                            60.0,
                            1000.0
                        ]
                    }
                ]
            },
            u"temp": 234.0
        }
    }, 
    {
        u"$group": {
            u"_id": {
                u"user_id": u"$player"
            },
            u"contacts": {
                u"$push": u"$$ROOT"
            },
            u"count": {
                u"$sum": 1.0
            },
            u"times": {
                u"$push": u"$ts"
            }
        }
    }, 
    {
        u"$project": {
            u"contacts": 1.0
        }
    }, 
    {
        u"$unwind": {
            u"path": u"$contacts",
            u"includeArrayIndex": u"arrayIndex",
            u"preserveNullAndEmptyArrays": False
        }
    }
]

cursor = collection.aggregate(
    pipeline, 
    allowDiskUse = True
)
try:
    for doc in cursor:
        print(doc)
finally:
    client.close()
