import os
from pathlib import Path
from typing import Optional
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

from tinkoff.invest import Client, CandleInterval
from tinkoff.invest.utils import quotation_to_decimal
from typing import Optional
from tinkoff.invest.utils import now

from utils import load_env_from_caller


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
        # df.to_parquet(filename, index=False)

        # print(f"Successfully saved {len(df)} candles to {filename}")
        return filename, df


class DataBaseConnector:
    def __init__(
        self,
        database_url: Optional[str] = None,
        host: Optional[str] = 'localhost',
        port: Optional[int] = 5432,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None):
        """
        Создаёт подключение к PostgreSQL.
        
        Приоритет:
        1. Если передан `database_url` — используется он.
        2. Иначе — если переданы отдельные параметры (host, user и т.д.) — используются они.
        3. Иначе — берётся DATABASE_URL из переменных окружения.
        """
        # Шаг 1: определяем источник конфигурации
        if database_url is not None:
            # Явно передан URL → используем его
            self.connection_params = database_url
        elif all(x is not None for x in [host, port, database, user, password]):
            # Переданы все отдельные параметры → собираем dict
            self.connection_params = {
                "host": host,
                "port": port,
                "database": database,
                "user": user,
                "password": password,
            }
        else:
            # Ничего не передано → берём из окружения
            env_url = os.getenv("DATABASE_URL")
            if not env_url:
                raise ValueError(
                    "DATABASE_URL не задан в переменных окружения, "
                    "и не переданы параметры подключения."
                )
            self.connection_params = env_url

    def connect(self):
        """Создаёт и возвращает соединение."""
        if isinstance(self.connection_params, str):
            # Подключение по URL
            return psycopg2.connect(self.connection_params)
        else:
            # Подключение по dict
            return psycopg2.connect(**self.connection_params)

    def execute(self, query: str, params=None):
        """Пример метода для выполнения запроса."""
        conn = self.connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if cur.description:  # SELECT
                        return cur.fetchall()
        finally:
            conn.close()
    
    def save_data(self, data):
        conn = self.connect()
        cur = conn.cursor()
        
        records = [
            (row.time, row.open, row.high, row.low, row.close, row.volume)
            for _, row in data.iterrows()]
        
        try:
            cur.executemany("""
                INSERT INTO tdata (date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)""", records)
            conn.commit()
            print('Data saved')
        except:
            print('error')
        finally:
            conn.close()


if __name__ == "__main__":
    root_dir = load_env_from_caller()
    TOKEN = os.getenv('TINKOFF_TOKEN')
    DB_URL = os.getenv('DB_URL')
    base_dir = root_dir / 'data/raw/prices'
    FIGI = "BBG004730N88"  # SBER

    downloader = HistoricalDataDownloader(
        token=TOKEN,
        figi=FIGI,
        days_back=365*5,
        interval='1w')
    _, data = downloader.download_data()
    
    connector = DataBaseConnector(DB_URL)
    connector.save_data(data)
        
    # for interval in ['1d', '1w', '1m']:
    #     downloader = HistoricalDataDownloader(
    #         token=TOKEN,
    #         figi=FIGI,
    #         days_back=365*5,
    #         interval=interval,
    #         base_dir=base_dir)
    #     downloader.download_data()
