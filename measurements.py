# Functions that take a mongo collection and return some measurements #

def getNumWords(collection):
    return collection.count_documents({})

def getNumLinks(collection):
    
    # Get sum of urls list lengths
    result = list(collection.aggregate([{"$project": {"word": 1, "count": {"$size": "$urls"}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}} }]))
    if len(result) > 0:
        return result[0]["total"]
    else:
        return 0
