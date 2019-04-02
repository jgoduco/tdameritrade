import os
from enum import Flag, auto
import requests
import pandas as pd
from .urls import ACCOUNTS, INSTRUMENTS, QUOTES, SEARCH, HISTORY, OPTIONCHAIN


class AccountInfo(Flag):
    BALANCES = auto()
    POSITIONS = auto()
    ORDERS = auto()
    ALL = BALANCES | POSITIONS | ORDERS


class TDClient(object):
    def __init__(self, access_token=None, accountIds=None):
        self._token = access_token or os.environ['ACCESS_TOKEN']
        self.accountIds = accountIds or []

    def _headers(self):
        return {'Authorization': 'Bearer ' + self._token}

    # for backwards compatibility, the default query param is BALANCES only
    def accounts(self, user_params=AccountInfo.BALANCES):
        fields = []
        if user_params & AccountInfo.POSITIONS:
            fields.append('positions')
        if user_params & AccountInfo.ORDERS:
            fields.append('orders')

        ret = {}
        paramsDict = {'fields': ','.join(fields)}
        if self.accountIds:
            for acc in self.accountIds:
                resp = requests.get(ACCOUNTS + str(acc), headers=self._headers(), params=paramsDict)
                if resp.status_code == 200:
                    ret[acc] = resp.json()
                else:
                    raise Exception(resp.text)
        else:
            resp = requests.get(ACCOUNTS, headers=self._headers(), params=paramsDict)
            if resp.status_code == 200:
                for account in resp.json():
                    ret[account['securitiesAccount']['accountId']] = account
            else:
                raise Exception(resp.text)
        return ret

    def accountsDF(self, user_params=AccountInfo.BALANCES):
        return pd.io.json.json_normalize(self.accounts(user_params))

    def search(self, symbol, projection='symbol-search'):
        return requests.get(SEARCH,
                            headers=self._headers(),
                            params={'symbol': symbol,
                                    'projection': projection}).json()

    def searchDF(self, symbol, projection='symbol-search'):
        ret = []
        dat = self.search(symbol, projection)
        for symbol in dat:
            ret.append(dat[symbol])
        return pd.DataFrame(ret)

    def fundamental(self, symbol):
        return self.search(symbol, 'fundamental')

    def fundamentalDF(self, symbol):
        return self.searchDF(symbol, 'fundamental')

    def instrument(self, cusip):
        return requests.get(INSTRUMENTS + str(cusip),
                            headers=self._headers()).json()

    def instrumentDF(self, cusip):
        return pd.DataFrame(self.instrument(cusip))

    # symbol can be a string | or string[]
    # eg. 'aapl' | ['appl', 'csco', 'msft']
    def quote(self, symbol):
        if isinstance(symbol, (list,)):
            symbol = ','.join(symbol)
        return requests.get(QUOTES,
                            headers=self._headers(),
                            params={'symbol': symbol.upper()}).json()

    def quoteDF(self, symbol):
        x = self.quote(symbol)
        return pd.DataFrame(x).T.reset_index(drop=True)

    def history(self, symbol):
        return requests.get(HISTORY % symbol,
                            headers=self._headers()).json()

    def historyDF(self, symbol):
        x = self.history(symbol)
        df = pd.DataFrame(x['candles'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        return df

    def options(self, symbol):
        return requests.get(OPTIONCHAIN,
                            headers=self._headers(),
                            params={'symbol': symbol.upper()}).json()

    def optionsDF(self, symbol):
        ret = []
        dat = self.options(symbol)
        for date in dat['callExpDateMap']:
            for strike in dat['callExpDateMap'][date]:
                ret.extend(dat['callExpDateMap'][date][strike])
        for date in dat['putExpDateMap']:
            for strike in dat['putExpDateMap'][date]:
                ret.extend(dat['putExpDateMap'][date][strike])

        df = pd.DataFrame(ret)
        for col in ('tradeTimeInLong', 'quoteTimeInLong', 'expirationDate', 'lastTradingDay'):
            df[col] = pd.to_datetime(df[col], unit='ms')
        return df
