import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

from tinkoff.invest import Client, CandleInterval
from tinkoff.invest.utils import quotation_to_decimal
from typing import Optional
from tinkoff.invest.utils import now


KEY = '/home/mltx/Documents/Projects/goblin-v1/config/api_keys.env'


class HistoricalDataDownloader:
    def __init__(self, token: str, figi: str, days_back: int = 365, interval: str = '1d'):
        self.token = token
        self.figi = figi
        self.days_back = days_back
        self.base_dir = '/home/mltx/Documents/Projects/goblin-v1/data/raw/prices/'
        self.interval = {
            '1d': CandleInterval.CANDLE_INTERVAL_DAY,
            '1w': CandleInterval.CANDLE_INTERVAL_WEEK,
            '1m': CandleInterval.CANDLE_INTERVAL_MONTH
        }[interval]
        os.makedirs(self.base_dir, exist_ok=True)

    def _convert_candle(self, candle) -> dict:
        """Конвертирует свечу в словарь с автоматическим преобразованием quotation"""
        return {
            'time': candle.time,
            'open': float(quotation_to_decimal(candle.open)),
            'high': float(quotation_to_decimal(candle.high)),
            'low': float(quotation_to_decimal(candle.low)),
            'close': float(quotation_to_decimal(candle.close)),
            'volume': candle.volume,
        }

    def _get_filename(self, from_: datetime, to: datetime) -> str:
        """Генерирует имя файла на основе дат"""
        return os.path.join(
            self.base_dir,
            f"{self.figi}_{from_.strftime('%Y%m%d')}_{to.strftime('%Y%m%d')}_{self.interval.name.split('_')[-1]}.parquet"
        )

    def download_data(self) -> Optional[str]:
        """Загружает исторические данные и сохраняет в файл"""
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


if __name__ == "__main__":
    load_dotenv(KEY)
    TOKEN = os.getenv('TINKOFF_TOKEN')
    FIGI = "BBG004730N88"  # SBER

    for interval in ['1d', '1w', '1m']:
        downloader = HistoricalDataDownloader(token=TOKEN, figi=FIGI, days_back=365*5, interval=interval)
        downloader.download_data()

