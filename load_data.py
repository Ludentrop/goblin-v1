import os
import pandas as pd
from dotenv import load_dotenv
from datetime import timedelta

from tinkoff.invest import CandleInterval, Client
from tinkoff.invest.schemas import CandleSource
from tinkoff.invest.utils import now

load_dotenv('/home/mltx/goblin-v1/config/api_keys.env')
TOKEN = os.getenv('TINKOFF_TOKEN')


def main():
    with Client(TOKEN) as client:
        for candle in client.get_all_candles(
            instrument_id="BBG004730N88",
            from_=now() - timedelta(days=365),
            interval=CandleInterval.CANDLE_INTERVAL_HOUR,
            candle_source_type=CandleSource.CANDLE_SOURCE_UNSPECIFIED,
        ):
            pass

    print(dir(client.get_all_candles()[0]))
    return 0


if __name__ == "__main__":
    main()
