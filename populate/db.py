from pymongo import MongoClient
def db_session(func):
    result = [None]

    def wrapper(*args):
        def callback(s):
            result[0] = func(s, args[0].get_database(s))

        with args[0].get_session() as s:
            s.with_transaction(callback)

        return result[0]

    return wrapper


class DbOperation:
    def __init__(self, session, database):
        self.session = session
        self.database = database

    def connect(self):
        self.database.client = MongoClient(self.database.auth["url"])

    def check_collection_exists(self, collection_name, operation_name="CHECK COLLECTION EXISTS"):
        return collection_name in self.database.list_collection_names()

    def get_collection(self, collection_name, operation_name="CREATE COLLECTION"):
        return self.database[collection_name]

    def drop_collection(self, collection, operation_name="DROP COLLECTION"):
        return collection.drop()

    def insert_data(self, collection, data, operation_name="INSERT DATA"):
        return collection.insert_many(data, session=self.session)

    def update_data(self, collection, data, operation_name="UPDATE DATA"):
        return collection.update({"_id": data["_id"]}, data)

    def delete_data(self, collection, data, operation_name="DELETE DATA"):
        return collection.delete_one({"_id": data["_id"]})

    def close(self):
        pass


class Db:
    def __init__(self, auth):
        self.auth = auth
        self.client = None

    def connect(self):
        DbOperation(None, self).connect()

    def disconnect(self):
        DbOperation(None, self).close()
