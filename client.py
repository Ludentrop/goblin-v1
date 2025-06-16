import os

from tinkoff.invest import Client

TOKEN = "t.JqXvwKj6qwcK3PmXGnjVNbifgwS7-PeITSdI_Tsx7YtRbrD04nFzgsr35BayAz2rcP5mry7M78vf_NcC3bbsTw"  # os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        print(client.users.get_accounts())


if __name__ == "__main__":
    main()
