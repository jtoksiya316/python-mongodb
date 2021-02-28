from MongoDBUtils import MongoDBUtils as MDBU

if __name__ == '__main__':
    try:
        obj_mongo = MDBU("localhost")
        obj_mongo.connect_to_db("Performance")
        obj_mongo.get_documents_from_collection("Windows")
        filter_data = {"overallCPU": 65}
        print(obj_mongo.fetch_data_in_collection(filter_data, header='description'))
    except Exception as e:
        print(e)
