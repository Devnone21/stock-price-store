from xtb_init import symbols, timeframes, accounts, user
from xtb.XTBApi.api import Client
from xtb.XTBApi.exceptions import CommandFailed
from classes import Mongo
from datetime import datetime, date, time, timedelta
from time import sleep
import logging
logger = logging.getLogger('xtb.store')
logger.setLevel(logging.DEBUG)


class CandlesTime:
    def __init__(self, symbol: str, timeframe: int) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.name = f'real_{symbol}_{timeframe}'
        now = datetime.utcnow()
        self.max_backdate = now.date() - timedelta(days=12*timeframe)
        self.last_backdate = now.date()
        if timeframe == 30:
            self.max_backdate = max(self.max_backdate, date(2023, 7, 21))

    def get_candles_time(self, db: Mongo):
        docs = db.find_all('candles_time')
        for doc in docs:
            if doc['candles'] == self.name:
                self.last_backdate = date.fromisoformat(doc['last_backdate'])

    def update_candles_time(self, db: Mongo):
        doc = {
            '_id': self.name,
            'candles': self.name,
            'last_backdate': self.last_backdate.isoformat()
        }
        db.upsert_one('candles_time', match={'candles': self.name}, data=doc)
        # print(f"upsert: {n_upsert}, last_backdate: {self.last_backdate}")


class Collection:
    def __init__(self, db: Mongo) -> None:
        self.db = db

    def collect_market_hours(self, client):
        res = client.get_trading_hours(symbols)
        logger.debug(f'recv market_hours {len(res)} symbols.')
        if not res:
            return
        self.db.insert_list_of_dict(
            collection='market_hours',
            data=[dict(d, **{'_id': d.get('symbol')}) for d in res]
        )

    def collect_candles(self, client, symbol: str, timeframe: int):
        ct = CandlesTime(symbol, timeframe)
        ct.get_candles_time(self.db)

        # get present charts
        ts = int(datetime.utcnow().timestamp())
        res = client.get_chart_range_request(symbol, timeframe, ts, ts, -100) if client else {}
        rate_infos = res.get('rateInfos', [])
        logger.info(f'recv {symbol}_{timeframe} {len(rate_infos)} ticks.')
        # get backdate charts
        min_ts = 0
        if ct.max_backdate < ct.last_backdate:
            sleep(1)
            ts = int(datetime.combine(ct.last_backdate, time(0, 0)).timestamp())
            res = client.get_chart_range_request(symbol, timeframe, ts, ts, -500) if client else {}
            backdate_infos = res.get('rateInfos', [])
            min_ts = min([int(c['ctm']) for c in backdate_infos]) / 1000
            logger.info(f'recv {symbol}_{timeframe} {len(backdate_infos)} backdate ticks.')
            # combine received charts
            rate_infos.extend(backdate_infos)

        if not rate_infos:
            return

        # store in DB/Cache
        n_inserted = self.db.insert_list_of_dict(
            collection=f'real_{symbol}_{timeframe}',
            data=[dict(d, **{'_id': d.get('ctm')}) for d in rate_infos]
        )
        # update last backdate
        if n_inserted >= 0 and min_ts > datetime(2020, 7, 1).timestamp():
            ct.last_backdate = date.fromtimestamp(min_ts) + timedelta(days=1)
            ct.update_candles_time(self.db)
        # summary
        logger.info(
            f'store {symbol}_{timeframe}: {n_inserted} ticks. ' +
            f'backdate: {ct.last_backdate.isoformat()}'
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
    market = collection.db.find_all('market_hours')
    set_market = {doc['symbol'] for doc in market} if market else set()
    if not set(symbols).issubset(set_market):
        collection.collect_market_hours(client=xtb)

    # Collect candles
    for symbol in symbols:
        for timeframe in timeframes:
            collection.collect_candles(
                client=xtb,
                symbol=symbol,
                timeframe=timeframe,
            )
            sleep(1)
        # Delay before next symbol
        sleep(3)

    db.client.close()
    xtb.logout()


if __name__ == '__main__':
    collect()
