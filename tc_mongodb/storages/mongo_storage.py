# -*- coding: utf-8 -*-
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor-Community
# Copyright (c) 2011 globo.com timehome@corp.globo.com

from datetime import datetime, timedelta
import gridfs
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError
from tornado.concurrent import return_future
from thumbor.storages import BaseStorage
from thumbor.utils import logger
from tc_mongodb.utils import OnException


class Storage(BaseStorage):

    def __init__(self, context):
        '''Initialize the MongoStorage

        :param thumbor.context.Context shared_client: Current context
        '''
        BaseStorage.__init__(self, context)
        self.database, self.storage = self.__conn__()
        self.ensure_index()
        super(Storage, self).__init__(context)

    def __conn__(self):
        '''Return the MongoDB database and collection object.
        :returns: MongoDB DB and Collection
        :rtype: pymongo.database.Database, pymongo.database.Collection
        '''
        if self.context.config.MONGO_STORAGE_URI:
            connection = MongoClient(self.context.config.MONGO_STORAGE_URI)
        else:
            connection = MongoClient(
                self.context.config.MONGO_STORAGE_SERVER_HOST,
                self.context.config.MONGO_STORAGE_SERVER_PORT
            )

        database = connection[self.context.config.MONGO_STORAGE_SERVER_DB]
        storage = database[self.context.config.MONGO_STORAGE_SERVER_COLLECTION]

        return database, storage

    def on_mongodb_error(self, fname, exc_type, exc_value):
        '''Callback executed when there is a redis error.
        :param string fname: Function name that was being called.
        :param type exc_type: Exception type
        :param Exception exc_value: The current exception
        :returns: Default value or raise the current exception
        '''

        if self.context.config.MONGODB_STORAGE_IGNORE_ERRORS:
            logger.error("[MONGODB_STORAGE] %s" % exc_value)
            if fname == '_exists':
                return False
            return None
        else:
            raise exc_value

    @OnException(on_mongodb_error, PyMongoError)
    def ensure_index(self):
        index_name = 'path_1_created_at_-1'
        if index_name not in self.storage.index_information():
            self.storage.create_index(
                [('path', ASCENDING), ('created_at', DESCENDING)],
                name=index_name
            )
        else:
            logger.info("[MONGODB_STORAGE] Index Already Exists")

    def get_max_age(self):
        '''Return the TTL of the current request.
        :returns: The TTL value for the current request.
        :rtype: int
        '''

        return self.context.config.STORAGE_EXPIRATION_SECONDS

    @OnException(on_mongodb_error, PyMongoError)
    def put(self, path, bytes):
        doc = {
            'path': path,
            'created_at': datetime.utcnow()
        }

        doc_with_crypto = dict(doc)
        if self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            if not self.context.server.security_key:
                raise RuntimeError(
                    "STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True \
                        if no SECURITY_KEY specified")
            doc_with_crypto['crypto'] = self.context.server.security_key

        fs = gridfs.GridFS(self.database)
        file_data = fs.put(bytes, **doc)

        doc_with_crypto['file_id'] = file_data
        self.storage.insert_one(doc_with_crypto)

    @OnException(on_mongodb_error, PyMongoError)
    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return None

        if not self.context.server.security_key:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be \
                True if no SECURITY_KEY specified")

        self.storage.update_one(
            {'path': path},
            {'$set': {'crypto': self.context.server.security_key}}
        )

    @OnException(on_mongodb_error, PyMongoError)
    def put_detector_data(self, path, data):
        self.storage.update({'path': path}, {"$set": {"detector_data": data}})

    @return_future
    def get_crypto(self, path, callback):
        callback(self._get_crypto(path))

    @OnException(on_mongodb_error, PyMongoError)
    def _get_crypto(self, path):
        crypto = self.storage.find_one({'path': path})
        return crypto.get('crypto') if crypto else None

    @return_future
    def get_detector_data(self, path, callback):
        callback(self._get_detector_data(path))

    @OnException(on_mongodb_error, PyMongoError)
    def _get_detector_data(self, path):
        doc = next(self.storage.find({
            'path': path,
            'detector_data': {'$ne': None},
        }, {
            'detector_data': True,
        }).limit(1), None)

        return doc.get('detector_data') if doc else None

    @return_future
    def get(self, path, callback):
        callback(self._get(path))

    @OnException(on_mongodb_error, PyMongoError)
    def _get(self, path):
        now = datetime.utcnow()
        stored = next(
            self.storage.find({
                'path': path,
                'created_at': {
                    '$gte': now - timedelta(seconds=self.get_max_age())},
            }, {'file_id': True}).limit(1), None
        )

        if not stored:
            return None

        fs = gridfs.GridFS(self.database)

        contents = fs.get(stored['file_id']).read()
        return contents

    @return_future
    def exists(self, path, callback):
        callback(self._exists(path))

    @OnException(on_mongodb_error, PyMongoError)
    def _exists(self, path):
        return self.storage.find({
            'path': path,
            'created_at': {
                '$gte':
                    datetime.utcnow() - timedelta(seconds=self.get_max_age())
            },
        }).limit(1).count() >= 1

    @OnException(on_mongodb_error, PyMongoError)
    def remove(self, path):
        self.storage.delete_many({'path': path})

        fs = gridfs.GridFS(self.database)
        file_datas = fs.find({'path': path})
        if file_datas:
            for file_data in file_datas:
                fs.delete(file_data._id)
