""" This Python script has a generic code for working with MongoDB.
    As per MongoDB,
        Collection == Table
        Document == records or each row in table
"""

from pymongo import MongoClient


class MongoDBUtils:

    def __init__(self, ip, port=27017):
        self.ip = ip
        self.port = port
        self.obj_collection = None
        self.obj_db = None

    def connect_to_db(self, db_name, db_uname, db_password):
        """
            This method connects to a Database
            :param db_name: Name of Database to connect
            :param db_uname: Username to connect to database
            :param db_password: Password for database
            :return: None
            :Exception: Raises exception is something is wrong in Connection string.
                        For wrong username and/or password, raises Authentication Failed
        """
        try:
            print('Connecting to MongoDB')
            self.obj_db = MongoClient("mongodb://{}:{}@{}:{}".format(db_uname, db_password, self.ip, self.port))\
                .get_database(db_name)
        except Exception as e:
            print('Error while connecting to MongoDB')
            raise e

    def get_documents_from_collection(self, collection_name):
        """
            This Method validates collection name and returns all documents(records) from collection(table) name.
            :param collection_name:
            :return: None
            :Exception: raises exception as "ns not found" if collection name is wrong or not present in DB
        """
        try:
            if self.obj_db.validate_collection(collection_name):
                self.obj_collection = self.obj_db.get_collection(collection_name)
        except Exception as e:
            raise e

    def fetch_data_in_collection(self, query_data, header='all'):
        """
            This Method fetches data based on query_data param.
            :param query_data: query in dictionary as condition based on which collection will be filtered
            :param header: It is header title whose value needs to be fetched using above query_data.
                           If "all", complete filtered documents(records) will be returned
            :return: List of filtered/queried data
        """
        try:
            ls_data = []
            for data in self.obj_collection.find(query_data):
                if header == 'all':
                    ls_data.append(data)
                else:
                    ls_data.append(data[header])
            print(ls_data)
            return ls_data
        except Exception as e:
            raise e
