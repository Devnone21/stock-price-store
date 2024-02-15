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

    def find_all(self, collection: str):
        try:
            db_collection = self.db[collection]
            with db_collection.find() as cursor:
                res = [doc for doc in cursor]
                if res:
                    logger.debug(f'({collection}) found')
            return res
        except TypeError as err:
            logger.error(err)

    def upsert_one(self, collection: str, match: dict, data: dict):
        n_upsert = -1
        try:
            db_collection = self.db[collection]
            res = db_collection.update_one(
                filter=match, update={'$set': data},
                upsert=True
            )
            n_upsert = res.modified_count
            logger.debug(f'({collection}) upsert: {match}')
        except AttributeError as err:
            logger.error(err)
        finally:
            return n_upsert

    def insert_list_of_dict(self, collection: str, data: list):
        n_inserted = -1
        try:
            db_collection = self.db[collection]
            res = db_collection.insert_many(data, ordered=False)
            n_inserted = len(res.inserted_ids)
            logger.debug(f'({collection}) nInserted: {n_inserted}')
        except BulkWriteError as err:
            n_errors = len(err.details.get('writeErrors'))
            n_inserted = int(err.details.get('nInserted'))
            logger.debug(f'({collection}) nInserted: {n_inserted}, writeErrors: {n_errors}')
        except AttributeError as err:
            logger.error(err)
        finally:
            return n_inserted
