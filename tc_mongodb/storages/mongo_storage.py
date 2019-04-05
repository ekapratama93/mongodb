# -*- coding: utf-8 -*-
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor-Community
# Copyright (c) 2011 globo.com timehome@corp.globo.com

from datetime import datetime, timedelta
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

import gridfs
from pymongo import MongoClient

from thumbor.storages import BaseStorage
from tornado.concurrent import return_future


class Storage(BaseStorage):

    def __init__(self, context):
        self.database, self.storage = self.__conn__()

    def __conn__(self):
        '''Return the MongoDB database and collection object.
        :returns: MongoDB DB and Collection
        :rtype: pymongo.database.Database, pymongo.database.Collection
        '''
        connection = MongoClient(
            self.context.config.MONGO_STORAGE_SERVER_HOST,
            self.context.config.MONGO_STORAGE_SERVER_PORT
        )

        database = connection[self.context.config.MONGO_STORAGE_SERVER_DB]
        storage = database[self.context.config.MONGO_STORAGE_SERVER_COLLECTION]

        return database, storage

    def get_max_age(self):
        '''Return the TTL of the current request.
        :returns: The TTL value for the current request.
        :rtype: int
        '''

        default_ttl = self.context.config.STORAGE_EXPIRATION_SECONDS
        if self.context.request.max_age == 0:
            return self.context.request.max_age

        return default_ttl

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
        file_data = fs.put(StringIO(bytes), **doc)

        doc_with_crypto['file_id'] = file_data
        self.storage.insert(doc_with_crypto)
        return path

    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return None

        if not self.context.server.security_key:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be \
                True if no SECURITY_KEY specified")

        self.storage.update_one(
            {'path': path}, {'crypto': self.context.server.security_key}
        )

        return path

    def put_detector_data(self, path, data):
        self.storage.update({'path': path}, {"$set": {"detector_data": data}})
        return path

    @return_future
    def get_crypto(self, path, callback):
        crypto = self.storage.find_one({'path': path})
        callback(crypto.get('crypto') if crypto else None)

    @return_future
    def get_detector_data(self, path, callback):
        doc = next(self.storage.find({
            'path': path,
            'detector_data': {'$ne': None},
        }, {
            'detector_data': True,
        }).limit(1), None)

        callback(doc.get('detector_data') if doc else None)

    @return_future
    def get(self, path, callback):
        stored = next(self.storage.find({
            'path': path,
            'created_at': {
                '$gte':
                    datetime.utcnow() - timedelta(seconds=self.get_max_age())
            },
        }, {
            'file_id': True,
        }).limit(1), None)

        if not stored:
            callback(None)
            return

        fs = gridfs.GridFS(self.database)

        contents = fs.get(stored['file_id']).read()

        callback(str(contents))

    @return_future
    def exists(self, path, callback):
        callback(self.storage.find({
            'path': path,
            'created_at': {
                '$gte':
                    datetime.utcnow() - timedelta(seconds=self.get_max_age())
            },
        }).limit(1).count() >= 1)

    def remove(self, path):
        if not self.exists(path):
            return

        self.storage.remove({'path': path})

        fs = gridfs.GridFS(self.database)
        file_id = fs.find_one({'path': path})._id
        fs.delete(file_id)
