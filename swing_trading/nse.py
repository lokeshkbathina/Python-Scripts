from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import numpy as np
import requests
import pandas as pd
from io import BytesIO, StringIO
import pandas_market_calendars as mcal
import datetime as dt


class nse_self:
    def __init__(self):
        self.dd_mm_yyyy = '%d-%m-%Y'
        self.ddmmyyyy = '%d%m%Y'
        self.default_header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        self.header = {
            "referer": "https://www.nseindia.com/",
             "Connection": "keep-alive",
             "Cache-Control": "max-age=0",
             "DNT": "1",
             "Upgrade-Insecure-Requests": "1",
             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
             "Sec-Fetch-User": "?1",
             "Accept": "ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
             "Sec-Fetch-Site": "none",
             "Sec-Fetch-Mode": "navigate",
             "Accept-Language": "en-US,en;q=0.9,hi;q=0.8"
            }
        self.columns = ['Symbol', 'Series', 'Date', 'PrevClose', 'OpenPrice', 'HighPrice',
                            'LowPrice', 'LastPrice', 'ClosePrice', 'AveragePrice', 
                            'TotalTradedQuantity', 'TurnoverInRs', 'No.ofTrades', 
                            'DeliverableQty', '%DlyQttoTradedQty']
        self.nse_calendar = mcal.get_calendar("NSE") 


    def nse_urlfetch(self, url, origin_url="http://nseindia.com"):
        r_session = requests.session()
        nse_live = r_session.get(origin_url, headers=self.default_header)
        cookies = nse_live.cookies
        return r_session.get(url, headers=self.header, cookies=cookies)
    

    def derive_from_and_to_date(self, from_date: str = None, to_date: str = None, period: str = None):
        if not period:
            return from_date, to_date
        today = date.today()
        conditions = [period.upper() == '1D',
                    period.upper() == '1W',
                    period.upper() == '1M',
                    period.upper() == '6M',
                    period.upper() == '1Y'
                    ]
        value = [today - timedelta(days=1),
                today - timedelta(weeks=1),
                today - relativedelta(months=1),
                today - relativedelta(months=6),
                today - relativedelta(months=12)]

        f_date = np.select(conditions, value, default=(today - timedelta(days=1)))
        f_date = pd.to_datetime(str(f_date))
        while True:
            date_chk = self.nse_calendar.schedule(start_date=f_date, end_date=f_date)
            if not date_chk.empty:  # If market was open on this day
                break  # Stop the loop
            f_date -= timedelta(days=1)
        from_date = f_date.strftime(self.dd_mm_yyyy)
        today = today.strftime(self.dd_mm_yyyy)
        return from_date, today


    def get_nse_equities_by_date(self, trade_date: str):
        """
        parameters:
            trade_date: example '20-06-2023'

        returns:
            pd df
        """
        trade_date = datetime.strptime(trade_date, self.dd_mm_yyyy)
        use_date = trade_date.strftime(self.ddmmyyyy)

        url = f'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{use_date}.csv'
        request_bhav = self.nse_urlfetch(url)
        if request_bhav.status_code == 200:
            bhav_df = pd.read_csv(BytesIO(request_bhav.content))
        else:
            raise FileNotFoundError(f' Data not found, change the trade_date...')
        bhav_df.columns = [name.replace(' ', '') for name in bhav_df.columns]
        bhav_df['SERIES'] = bhav_df['SERIES'].str.replace(' ', '')
        bhav_df['DATE1'] = bhav_df['DATE1'].str.replace(' ', '')
        return bhav_df
    

    def get_price_volume_and_deliverable_position_data(self, symbol: str, from_date: str, to_date: str):
        origin_url = "https://nsewebsite-staging.nseindia.com/report-detail/eq_security"
        url = "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?"
        payload = f"from={from_date}&to={to_date}&symbol={symbol}&type=priceVolumeDeliverable&series=ALL&csv=true"
        try:
            data_text = self.nse_urlfetch(url + payload, origin_url=origin_url).text
            data_text = data_text.replace('\x82', '').replace('â¹', 'In Rs')
            data_df = pd.read_csv(StringIO(data_text))
        except Exception as e:
            raise (f" Resource not available MSG: {e}")
        data_df.columns = [name.replace(' ', '') for name in data_df.columns]
        return data_df
    

    def get_nse_stock_by_duration(self, symbol: str, period: str = None):
        """
        parameters:
            symbol: 'CUB'
            period: '1D', '1W', '1M', '6M', '1Y'

        returns:
            pd.df
        """
        from_date, to_date = self.derive_from_and_to_date(from_date=None, to_date=None, period=period)
        nse_df = pd.DataFrame(columns=self.columns)
        from_date = datetime.strptime(from_date, self.dd_mm_yyyy)
        to_date = datetime.strptime(to_date, self.dd_mm_yyyy)
        load_days = (to_date - from_date).days
        while load_days > 0:
            if load_days > 365:
                end_date = (from_date + dt.timedelta(364)).strftime(self.dd_mm_yyyy)
                start_date = from_date.strftime(self.dd_mm_yyyy)
            else:
                end_date = to_date.strftime(self.dd_mm_yyyy)
                start_date = from_date.strftime(self.dd_mm_yyyy)
            data_df = self.get_price_volume_and_deliverable_position_data(symbol=symbol, from_date=start_date, to_date=end_date)
            from_date = from_date + dt.timedelta(365)
            load_days = (to_date - from_date).days
            if not data_df.empty and not data_df.isna().all().all():
                nse_df = pd.concat([nse_df, data_df], ignore_index=True)

        nse_df["TotalTradedQuantity"] = pd.to_numeric(nse_df["TotalTradedQuantity"].str.replace(",", ""), errors="coerce")
        nse_df["TurnoverInRs"] = pd.to_numeric(nse_df["TurnoverInRs"].str.replace(",", ""), errors="coerce")
        nse_df["No.ofTrades"] = pd.to_numeric(nse_df["No.ofTrades"].str.replace(",", ""), errors="coerce")
        nse_df["DeliverableQty"] = pd.to_numeric(nse_df["DeliverableQty"].str.replace(",", ""), errors="coerce")
        return nse_df

if __name__ == '__main__':
    print(">>> Running updated nse_self.py")

    obj = nse_self()
    # df = obj.get_nse_equities_by_date('30-06-2025')
    # print(df.head(5))
    df = obj.get_nse_stock_by_duration('CUB', '1M')
    print(df)