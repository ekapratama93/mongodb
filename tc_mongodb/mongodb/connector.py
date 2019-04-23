from pymongo import ASCENDING, DESCENDING, MongoClient


class Singleton(type):
    """
    Define an Instance operation that lets clients access its unique
    instance.
    """

    def __init__(cls, name, bases, attrs, **kwargs):
        super(Singleton, cls).__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance


class MongoConnector(object):
    __metaclass__ = Singleton

    def __init__(self,
                 uri=None,
                 host=None,
                 port=None,
                 db_name=None,
                 coll_name=None,
                 result=False):
        self.uri = uri
        self.host = host
        self.port = port
        self.db_name = db_name
        self.coll_name = coll_name
        self.db_conn, self.coll_conn = self.create_connection()
        self.result = result
        self.ensure_index()

    def create_connection(self):
        if self.uri:
            connection = MongoClient(self.uri)
        else:
            connection = MongoClient(self.host, self.port)

        db_conn = connection[self.db_name]
        coll_conn = db_conn[self.coll_name]

        return db_conn, coll_conn

    def ensure_index(self):
        if self.result:
            index_name = 'key_1_created_at_-1'
            if index_name not in self.coll_conn.index_information():
                self.coll_conn.create_index(
                    [('key', ASCENDING), ('created_at', DESCENDING)],
                    name=index_name
                )
        else:
            index_name = 'path_1_created_at_-1'
            if index_name not in self.coll_conn.index_information():
                self.coll_conn.create_index(
                    [('path', ASCENDING), ('created_at', DESCENDING)],
                    name=index_name
                )
