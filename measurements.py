# Functions that take a mongo collection and return some measurements #

def getNumWords(collection):
    return collection.count_documents({})

def getNumLinks(collection):
    
    # Get sum of urls list lengths
    result = list(collection.aggregate([{"$project": {"urls": 1}}, {"$unwind": "$urls"}, {"$group": {"_id": None, "set": {"$addToSet": "$urls"}} }]))
    if len(result) > 0:
        return len(result[0]["set"])
    else:
        return 0

def clearCollection(collection):
    collection.remove({})
