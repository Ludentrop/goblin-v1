import asyncio
import os

from tinkoff.invest import AsyncClient

TOKEN = "t.JqXvwKj6qwcK3PmXGnjVNbifgwS7-PeITSdI_Tsx7YtRbrD04nFzgsr35BayAz2rcP5mry7M78vf_NcC3bbsTw"  # os.environ["INVEST_TOKEN"]


async def main():
    async with AsyncClient(TOKEN) as client:
        print(await client.users.get_accounts())


if __name__ == "__main__":
    asyncio.run(main())
