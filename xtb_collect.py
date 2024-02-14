from xtb_init import symbols, timeframes, accounts, user
from xtb.XTBApi.api import Client
from xtb.XTBApi.exceptions import CommandFailed
from classes import Mongo
from datetime import datetime
import time
import logging
logger = logging.getLogger('xtb.store')
logger.setLevel(logging.DEBUG)


class Collection:
    def __init__(self, db: Mongo) -> None:
        self.db = db

    def store_market_hours(self, client):
        res = client.get_trading_hours(symbols)
        logger.debug(f'recv market_hours {len(res)} symbols.')
        if not res:
            return
        self.db.insert_list_of_dict(
            collection='market_hours',
            data=[dict(d, **{'_id': d.get('symbol')}) for d in res]
        )

    def store_candles(self, client, symbol, timeframe):
        # get charts
        now = int(datetime.now().timestamp())
        res = client.get_chart_range_request(symbol, timeframe, now, now, -100) if client else {}
        rate_infos = res.get('rateInfos', [])
        logger.debug(f'recv {symbol}_{timeframe} {len(rate_infos)} ticks.')
        if not rate_infos:
            return
        # store in DB/Cache
        self.db.insert_list_of_dict(
            collection=f'real_{symbol}_{timeframe}',
            data=[dict(d, **{'_id': d.get('ctm')}) for d in rate_infos]
        )


def collect() -> None:
    # Get account information
    account: dict = accounts.get(user, {})
    if not account:
        return
    secret = account.get('pass', '')

    # Start X connection
    xtb = Client()
    try:
        xtb.login(user, secret, mode='real')
    except CommandFailed:
        logger.debug('Gate is closed.')
    logger.debug('Enter the Gate.')

    # Start DB connection
    db = Mongo(db='xtb')
    collection = Collection(db)

    # Collect market hours if not fully available
    market = collection.db.find_in('market_hours')
    set_market = {doc['symbol'] for doc in market} if market else set()
    if not set(symbols).issubset(set_market):
        collection.store_market_hours(client=xtb)

    # Collect candles
    for symbol in symbols:
        for timeframe in timeframes:
            collection.store_candles(
                client=xtb,
                symbol=symbol,
                timeframe=timeframe,
            )
        # Delay before next symbol
        time.sleep(5)

    xtb.logout()


if __name__ == '__main__':
    collect()
