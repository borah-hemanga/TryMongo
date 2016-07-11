from pymongo import MongoClient

MONGO_HOST = "mongodb://127.0.0.1:27017"
COLLECTION_NAME_1 = 'some_collection'

class PymongoClient(object):
    '''
    Connection handler for mongodb
    '''
    def __init__(self, db_name):
        self._client = MongoClient(MONGO_HOST)
        self._db = self._client[db_name]

    def insert(self, coll, json_doc):
        result = self._db[coll].insert_one(json_doc)

    def get_all(self, coll):
        return self._db[coll].find()

    def delete_all(self, coll):
        result = self._db[coll].delete_many({}) 

    def find(self, coll, param, val):
        return self._db[coll].find({param : val})
        

def run_tests(p):
    p.insert(COLLECTION_NAME_1, {'A' : 'B', 'B_1' : { 'C' : 'D', 'E' : 'F' }, 'B_2' : 'Simple'}) 
    p.insert(COLLECTION_NAME_1, {'A' : 'C'})
    p.insert(COLLECTION_NAME_1, {'D' : 'E'})
    f = p.get_all(COLLECTION_NAME_1)
    print "Get all"
    for doc in f:
        print doc
    print "Query"
    f = p.find(COLLECTION_NAME_1, 'B_1.E', 'F')
    for doc in f:
        print doc

    p.delete_all(COLLECTION_NAME_1)

if __name__ == '__main__':
    p = PymongoClient('sample_db')
    run_tests(p) 
