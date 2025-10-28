from dotenv import load_dotenv
import asyncio
import os

from tinkoff.invest import (
    AsyncClient,
    CandleInstrument,
    InfoInstrument,
    MarketDataResponse,
    SubscriptionInterval,
    TradeInstrument,
)
from tinkoff.invest.async_services import AsyncMarketDataStreamManager


KEY = '/home/mltx/Documents/Projects/goblin-v1/config/api_keys.env'

load_dotenv(KEY)
TOKEN = os.getenv('TINKOFF_TOKEN')


async def main():
    async with AsyncClient(TOKEN) as client:
        market_data_stream: AsyncMarketDataStreamManager = (
            client.create_market_data_stream()
        )
        market_data_stream.candles.waiting_close().subscribe(
            [
                CandleInstrument(
                    figi="BBG004730N88",
                    interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES,
                )
            ]
        )
        market_data_stream.last_price.subscribe(
            [
                TradeInstrument(
                    figi="BBG004730N88",
                )
            ]
        )
        async for marketdata in market_data_stream:
            marketdata: MarketDataResponse = marketdata
            print(marketdata)
            print()
            #market_data_stream.info.subscribe([InfoInstrument(figi="BBG004730N88")])
            #if marketdata.subscribe_info_response:
              #  market_data_stream.stop()


if __name__ == "__main__":
    asyncio.run(main())
