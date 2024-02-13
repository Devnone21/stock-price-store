from xtb_init import symbols, timeframes, accounts, user
from xtb.XTBApi.api import Client
from classes import Cache
from datetime import datetime
import time
import logging
logger = logging.getLogger('xtb.store')
logger.setLevel(logging.DEBUG)


class Result:
    def __init__(self, symbol: str, timeframe: int) -> None:
        self.symbol = symbol
        self.timeframe = timeframe

    def store_candles(self, client=None):
        # get charts
        now = int(datetime.now().timestamp())
        res = client.get_chart_range_request(self.symbol, self.timeframe, now, now, -10_000) if client else {}
        rate_infos = res.get('rateInfos', [])
        logger.debug(f'recv {self.symbol} {len(rate_infos)} ticks.')
        # store in DB/Cache
        try:
            cache = Cache()
            key_group = f'xtb_real_{self.symbol}_{self.timeframe}'
            for ctm in rate_infos:
                cache.set_key(f'{key_group}:{ctm["ctm"]}', ctm, ttl_s=self.timeframe*172_800)
        except ConnectionError as e:
            logger.error(e)


def collect() -> None:
    account: dict = accounts.get(user, {})
    if not account:
        return
    secret = account.get('pass', '')

    # Start X connection
    client = Client()
    client.login(user, secret, mode='real')
    logger.debug('Enter the Gate.')

    # Check if market is open
    market_status = client.get_market_status(symbols)
    logger.info(f'[Store-{user}] Market status: {market_status}')
    for symbol, status in market_status.items():
        if not status:
            continue

        # Market open, collect cd
        for tf in timeframes:
            r = Result(symbol, tf)
            r.store_candles(client=client)

        # Delay before next symbol
        time.sleep(5)

    client.logout()


if __name__ == '__main__':
    collect()
