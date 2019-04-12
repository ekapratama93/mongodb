# -*- coding: utf-8 -*-
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor-Community

import time
from datetime import datetime, timedelta

import gridfs
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from thumbor.result_storages import BaseStorage
from thumbor.utils import logger
from tc_mongodb.utils import OnException

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


class Storage(BaseStorage):

    '''start_time is used to calculate the last modified value when an item
    has no expiration date.
    '''
    start_time = None

    def __init__(self, context):
        self.database, self.storage = self.__conn__()

        if not Storage.start_time:
            Storage.start_time = time.time()

    def __conn__(self):
        connection = MongoClient(
            self.context.config.MONGO_RESULT_STORAGE_SERVER_HOST,
            self.context.config.MONGO_RESULT_STORAGE_SERVER_PORT
        )

        database = connection[
            self.context.config.MONGO_RESULT_STORAGE_SERVER_DB
        ]
        storage = database[
            self.context.config.MONGO_RESULT_STORAGE_SERVER_COLLECTION
        ]

        return database, storage

    def on_mongodb_error(self, fname, exc_type, exc_value):
        '''Callback executed when there is a redis error.
        :param string fname: Function name that was being called.
        :param type exc_type: Exception type
        :param Exception exc_value: The current exception
        :returns: Default value or raise the current exception
        '''

        logger.error("[MONGODB_STORAGE] %s" % exc_value)
        if fname == '_exists':
            return False
        return None

    def is_auto_webp(self):
        '''
        TODO This should be moved into the base storage class.
             It is shared with file_result_storage
        :return: If the file is a webp
        :rettype: boolean
        '''

        return self.context.config.AUTO_WEBP \
            and self.context.request.accepts_webp

    def get_key_from_request(self):
        '''Return a key for the current request url.
        :return: The storage key for the current url
        :rettype: string
        '''

        path = "result:%s" % self.context.request.url

        if self.is_auto_webp():
            path += '/webp'

        return path

    def get_max_age(self):
        '''Return the TTL of the current request.
        :returns: The TTL value for the current request.
        :rtype: int
        '''

        default_ttl = self.context.config.RESULT_STORAGE_EXPIRATION_SECONDS
        if self.context.request.max_age == 0:
            return self.context.request.max_age

        return default_ttl

    @OnException(on_mongodb_error, PyMongoError)
    def put(self, bytes):
        '''Save to mongodb
        :param bytes: Bytes to write to the storage.
        :return: MongoDB _id for the current url
        :rettype: string
        '''
        doc = {
            'key': self.get_key_from_request,
            'created_at': datetime.now()
        }

        file_doc = dict(doc)

        fs = gridfs.GridFS(self.database)
        file_data = fs.put(StringIO(bytes), **doc)

        file_doc['file_id'] = file_data
        self.database.storage.insert(file_doc)

        return self.get_key_from_request

    @OnException(on_mongodb_error, PyMongoError)
    def get(self):
        '''Get the item from MongoDB.'''

        key = self.get_key_from_request
        stored = next(self.storage.find({
            'key': key,
            'created_at': {'$gte': datetime.utcnow() - self.get_max_age()},
        }, {
            'file_id': True,
        }).limit(1), None)

        if not stored:
            return None

        fs = gridfs.GridFS(self.database)

        contents = fs.get(stored['file_id']).read()
        return str(contents)

    @OnException(on_mongodb_error, PyMongoError)
    def last_updated(self):
        '''Return the last_updated time of the current request item
        :return: A DateTime object
        :rettype: datetetime.datetime
        '''

        key = self.get_key_from_request
        max_age = self.get_max_age()

        if max_age == 0:
            return datetime.fromtimestamp(Storage.start_time)

        image = next(self.storage.find({
            'key': key,
            'created_at': {'$gte': datetime.utcnow() - self.get_max_age()},
        }, {
            'created_at': True, '_id': False
        }).limit(1), None)

        age = int((datetime.utcnow() - image['created_at']).total_seconds())
        ttl = max_age - age

        if max_age <= 0:
            return datetime.fromtimestamp(Storage.start_time)

        if ttl >= 0:
            return datetime.utcnow() - timedelta(
                seconds=(
                    max_age - ttl
                )
            )

        # Should never reach here. It means the storage put failed or the item
        # somehow does not exists anymore
        return datetime.utcnow()
