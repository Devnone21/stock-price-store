import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import logging
logger = logging.getLogger('xtb.store')
logger.setLevel(logging.DEBUG)


class Mongo:
    """class of Mongo DB client"""
    def __init__(self, db: str) -> None:
        self.client: MongoClient = MongoClient("mongodb://%s:%s@%s" % (
                os.getenv("MONGODB_USER"),
                os.getenv("MONGODB_PASS"),
                os.getenv("MONGODB_HOST"),
            )
        )
        self.db = self.client[db]

    def find_in(self, collection: str):
        try:
            db_collection = self.db[collection]
            res = db_collection.find()
            logger.debug(f'({collection}) found')
            return res
        except TypeError as err:
            logger.error(err)

    def insert_list_of_dict(self, collection: str, data: list):
        try:
            db_collection = self.db[collection]
            res = db_collection.insert_many(data, ordered=False)
            logger.debug(f'({collection}) nInserted: {len(res.inserted_ids)}')
        except BulkWriteError as err:
            n_errors = len(err.details.get('writeErrors'))
            n_inserted = err.details.get('nInserted')
            logger.error(f'({collection}) nInserted: {n_inserted}, writeErrors: {n_errors}')
        except AttributeError as err:
            logger.error(err)
