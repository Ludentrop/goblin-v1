import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import argparse
import logging

from tinkoff.invest import Client, CandleInterval
from tinkoff.invest.utils import quotation_to_decimal
from typing import Optional
from tinkoff.invest.utils import now

from utils import load_env_from_caller


logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        prog='fetch_hist_data_pq.py',
        description='Download price historical data.'
    )
    parser.add_argument('figi', type=str, help='FIGI of an asset (ex. BBG004730N88 - SBER).')
    parser.add_argument('year', type=int, help='For five years - 5.')
    parser.add_argument('interval', type=str, help='Interval: 1d, 1w, 1m.')
    return parser.parse_args()


def main(args):
    logger.info('Run main function.')
    logger.info('Loading environment variables.')
    
    root_dir = load_env_from_caller()
    base_dir = root_dir / 'data/raw/prices'
    TOKEN = os.getenv('TINKOFF_TOKEN')
    # FIGI = "BBG004730N88"  # SBER
    
    logger.info('Trying to fetch data.')
    
    downloader = HistoricalDataDownloader(
        token=TOKEN,
        figi=args.figi,
        days_back=365*args.year,
        interval=args.interval,
        base_dir=base_dir
    )
    downloader.download_data()


class HistoricalDataDownloader:
    def __init__(
        self,
        token: str,
        figi: str,
        days_back: int = 365,
        interval: str = '1d',
        base_dir=Path(__file__).parent):
            
        self.token = token
        self.figi = figi
        self.days_back = days_back
        self.base_dir = base_dir
        self.interval = {
            '1d': CandleInterval.CANDLE_INTERVAL_DAY,
            '1w': CandleInterval.CANDLE_INTERVAL_WEEK,
            '1m': CandleInterval.CANDLE_INTERVAL_MONTH
        }[interval]
        os.makedirs(self.base_dir, exist_ok=True)

    def _convert_candle(self, candle) -> dict:
        return {
            'time': candle.time,
            'open': float(quotation_to_decimal(candle.open)),
            'high': float(quotation_to_decimal(candle.high)),
            'low': float(quotation_to_decimal(candle.low)),
            'close': float(quotation_to_decimal(candle.close)),
            'volume': candle.volume,
        }

    def _get_filename(self, from_: datetime, to: datetime) -> str:
        return os.path.join(
            self.base_dir,
            f"{self.figi}_{from_.strftime('%Y%m%d')}_{to.strftime('%Y%m%d')}_{self.interval.name.split('_')[-1]}.parquet"
        )

    def download_data(self) -> Optional[str]:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=self.days_back)

        all_candles = []

        with Client(self.token) as client:
            for candle in client.get_all_candles(
                figi=self.figi,
                from_=start_date,
                to=end_date,
                interval=self.interval
            ):
                all_candles.append(self._convert_candle(candle))

        if not all_candles:
            print("No data received")
            return None

        df = pd.DataFrame(all_candles)
        filename = self._get_filename(start_date, end_date)
        df.to_parquet(filename, index=False)

        print(f"Successfully saved {len(df)} candles to {filename}")
        return filename


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    main(args)
