# -*- coding: utf-8 -*-
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor-Community
# Copyright (c) 2011 globo.com timehome@corp.globo.com

import datetime
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

from pymongo import MongoClient
import gridfs

from thumbor.storages import BaseStorage
from tornado.concurrent import return_future


class Storage(BaseStorage):

    def __conn__(self):
        connection = MongoClient(
            self.context.config.MONGO_STORAGE_SERVER_HOST,
            self.context.config.MONGO_STORAGE_SERVER_PORT
        )

        db = connection[self.context.config.MONGO_STORAGE_SERVER_DB]
        storage = db[self.context.config.MONGO_STORAGE_SERVER_COLLECTION]

        return db, storage

    def put(self, path, bytes):
        db, storage = self.__conn__()

        doc = {
            'path': path,
            'created_at': datetime.datetime.now()
        }

        doc_with_crypto = dict(doc)
        if self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            if not self.context.server.security_key:
                raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")
            doc_with_crypto['crypto'] = self.context.server.security_key

        fs = gridfs.GridFS(db)
        file_data = fs.put(StringIO(bytes), **doc)

        doc_with_crypto['file_id'] = file_data
        storage.insert(doc_with_crypto)
        return path

    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return

        db, storage = self.__conn__()

        if not self.context.server.security_key:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")

        storage.update_one({'path': path}, {'crypto': self.context.server.security_key})

        return path

    def put_detector_data(self, path, data):
        db, storage = self.__conn__()

        storage.update({'path': path}, {"$set": {"detector_data": data}})
        return path

    @return_future
    def get_crypto(self, path, callback):
        db, storage = self.__conn__()

        crypto = storage.find_one({'path': path})
        callback(crypto.get('crypto') if crypto else None)

    @return_future
    def get_detector_data(self, path, callback):
        db, storage = self.__conn__()

        doc = next(storage.find({
            'path': path,
            'detector_data': {'$ne': None},
        }, {
            'detector_data': True,
        }).limit(1), None)

        callback(doc.get('detector_data') if doc else None)

    @return_future
    def get(self, path, callback):
        db, storage = self.__conn__()

        stored = next(storage.find({
            'path': path,
            'created_at': {'$gte': datetime.datetime.utcnow() - datetime.timedelta(seconds=self.context.config.STORAGE_EXPIRATION_SECONDS)},
        }, {
            'file_id': True,
        }).limit(1), None)

        if not stored:
            callback(None)
            return

        fs = gridfs.GridFS(db)

        contents = fs.get(stored['file_id']).read()

        callback(str(contents))

    @return_future
    def exists(self, path, callback):
        db, storage = self.__conn__()

        callback(storage.find({
            'path': path,
            'created_at': {'$gte': datetime.datetime.utcnow() - datetime.timedelta(seconds=self.context.config.STORAGE_EXPIRATION_SECONDS)},
        }).limit(1).count() > 1)

    def remove(self, path):
        if not self.exists(path):
            return

        db, storage = self.__conn__()
        storage.remove({'path': path})

        fs = gridfs.GridFS(db)
        file_id = fs.find_one({'path': path})._id
        fs.delete(file_id)
