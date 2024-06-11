import os
import copy
import xlwings as xw
import pandas as pd
import numpy as np
from kiteconnect import KiteTicker
import time
import dateutil.parser
import threading
import sys
import requests
import pyotp

from py_vollib.black_scholes.implied_volatility import implied_volatility
from py_vollib.black_scholes.greeks.analytical import delta, gamma, rho, theta, vega
from datetime import datetime, timedelta
pd.set_option('future.no_silent_downcasting', True)

print('''
 Welcome to Option Chain
 Created by Gopal Das
 Contact No. 8620906459
      ''')

# print("----Option Chain----")
# sheet_no = input("Enter Sheet No. (Max 3 Run): ").strip()

if not os.path.exists("Option Chain - Gopal.xlsm"):
    try:
        wb = xw.Book()
        wb.save("Option Chain - Gopal.xlsm")
        wb.close()
    except Exception as e:
        print(f"Error Creating Excel File : {e}")
        sys.exit()
wb = xw.Book("Option Chain - Gopal.xlsm")

# try:
#     wb.sheets(f"OptionChain{sheet_no}")
# except:
#     wb.sheets.add(f"OptionChain{sheet_no}")
# oc = wb.sheets(f"OptionChain{sheet_no}")

try:
    oc = wb.sheets("Live")
    lc = wb.sheets("LICENCE")
except:
    wb.sheets.add("Live")
    oc = wb.sheets("Live")
    wb.sheets.add("LICENCE")
    lc = wb.sheets("LICENCE")
    lc.range("A1").value, lc.range("A2").value, lc.range("A3").value = "USER ID", "PASSWORD", "TOTP_KEY"
    lc.range('A1:B3').api.Borders.Weight = 3
    print("Fill The Required Details In Licence Sheet And Rerun The Software. Thank You!!!")
    time.sleep(100)
    sys.exit()


lc.range("A1").value, lc.range("A2").value, lc.range("A3").value = "USER ID", "PASSWORD", "TOTP_KEY"

oc.range("a:b").value = None

def login_with_credentials(userid, password, twofa):
    reqsession = requests.Session()
    r = reqsession.post('https://kite.zerodha.com/api/login', data={
        "user_id": userid,
        "password": password
    })

    r = reqsession.post('https://kite.zerodha.com/api/twofa', data={
        "request_id": r.json()['data']['request_id'],
        "twofa_value": twofa,
        "user_id": r.json()['data']['user_id']
    })
    enctoken = r.cookies.get('enctoken')
    return enctoken

userid = lc.range("b1").value
password = lc.range("b2").value
totp_key = str(lc.range("b3").value)

twofa = pyotp.TOTP(totp_key).now()
access_token = login_with_credentials(userid, password, twofa)


def get_object():
    global kite, access_token, user_id
    class KiteApp:
                # Products
                PRODUCT_MIS = "MIS"
                PRODUCT_CNC = "CNC"
                PRODUCT_NRML = "NRML"
                PRODUCT_CO = "CO"

                # Order types
                ORDER_TYPE_MARKET = "MARKET"
                ORDER_TYPE_LIMIT = "LIMIT"
                ORDER_TYPE_SLM = "SL-M"
                ORDER_TYPE_SL = "SL"

                # Varities
                VARIETY_REGULAR = "regular"
                VARIETY_CO = "co"
                VARIETY_AMO = "amo"

                # Transaction type
                TRANSACTION_TYPE_BUY = "BUY"
                TRANSACTION_TYPE_SELL = "SELL"

                # Validity
                VALIDITY_DAY = "DAY"
                VALIDITY_IOC = "IOC"

                # Exchanges
                EXCHANGE_NSE = "NSE"
                EXCHANGE_BSE = "BSE"
                EXCHANGE_NFO = "NFO"
                EXCHANGE_CDS = "CDS"
                EXCHANGE_BFO = "BFO"
                EXCHANGE_MCX = "MCX"

                def __init__(self, enctoken):
                    self.enctoken = enctoken
                    self.headers = {"Authorization": f"enctoken {self.enctoken}"}
                    self.session = requests.session()
                    self.root_url = "https://kite.zerodha.com/oms"
                    self.session.get(self.root_url, headers=self.headers)

                def instruments(self, exchange=None):
                    data = self.session.get(f"https://api.kite.trade/instruments").text.split("\n")
                    Exchange = []
                    for i in data[1:-1]:
                        row = i.split(",")
                        if exchange is None or exchange == row[11]:
                            Exchange.append(
                                {'instrument_token': int(row[0]), 'exchange_token': row[1], 'tradingsymbol': row[2],
                                 'name': row[3][1:-1], 'last_price': float(row[4]),
                                 'expiry': dateutil.parser.parse(row[5]).date() if row[5] != "" else None,
                                 'strike': float(row[6]), 'tick_size': float(row[7]), 'lot_size': int(row[8]),
                                 'instrument_type': row[9], 'segment': row[10],
                                 'exchange': row[11]})
                    return Exchange

                def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
                    params = {"from": from_date,
                              "to": to_date,
                              "interval": interval,
                              "continuous": 1 if continuous else 0,
                              "oi": 1 if oi else 0}
                    lst = self.session.get(
                        f"{self.root_url}/instruments/historical/{instrument_token}/{interval}", params=params,
                        headers=self.headers).json()["data"]["candles"]
                    records = []
                    for i in lst:
                        record = {"date": dateutil.parser.parse(i[0]), "open": i[1], "high": i[2], "low": i[3],
                                  "close": i[4], "volume": i[5], }
                        if len(i) == 7:
                            record["oi"] = i[6]
                        records.append(record)
                    return records

                def margins(self):
                    margins = self.session.get(f"{self.root_url}/user/margins", headers=self.headers).json()["data"]
                    return margins

                def profile(self):
                    profile = self.session.get(f"{self.root_url}/user/profile", headers=self.headers).json()["data"]
                    return profile

                def orders(self):
                    orders = self.session.get(f"{self.root_url}/orders", headers=self.headers).json()["data"]
                    return orders

                def positions(self):
                    positions = self.session.get(f"{self.root_url}/portfolio/positions", headers=self.headers).json()[
                        "data"]
                    return positions

                def place_order(self, variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type,
                                price=None,
                                validity=None, disclosed_quantity=None, trigger_price=None, squareoff=None,
                                stoploss=None,
                                trailing_stoploss=None, tag=None):
                    params = locals()
                    del params["self"]
                    for k in list(params.keys()):
                        if params[k] is None:
                            del params[k]
                    order_id = self.session.post(f"{self.root_url}/orders/{variety}",
                                                 data=params, headers=self.headers).json()["data"]["order_id"]
                    return order_id

                def modify_order(self, variety, order_id, parent_order_id=None, quantity=None, price=None,
                                 order_type=None,
                                 trigger_price=None, validity=None, disclosed_quantity=None):
                    params = locals()
                    del params["self"]
                    for k in list(params.keys()):
                        if params[k] is None:
                            del params[k]
                
                    order_id = self.session.put(f"{self.root_url}/orders/{variety}/{order_id}",
                                                data=params, headers=self.headers).json()["data"][
                        "order_id"]
                    return order_id

                def cancel_order(self, variety, order_id, parent_order_id=None):
                    order_id = self.session.delete(f"{self.root_url}/orders/{variety}/{order_id}",
                                                   data={"parent_order_id": parent_order_id} if parent_order_id else {},
                                                   headers=self.headers).json()["data"]["order_id"]
                    return order_id

    kite = KiteApp(enctoken=access_token)

    user_id = kite.profile()["user_id"]
    # print(f"Logged In : {user_id}")


def start_websocket():
    global access_token, user_id, kws, tick_data, token_symbol
    access_token = access_token+"&user_id="+user_id
    kws = KiteTicker(api_key="GopalShilpa", access_token=access_token)

    tick_data = {}
    token_symbol = {}


    def on_ticks(ws, ticks):
        for i in ticks:
            tick_data[token_symbol[i["instrument_token"]]] = i


    kws.on_ticks = on_ticks
    kws.connect(threaded=True)
    while not kws.is_connected():
        time.sleep(1)
    # print("WebSocket : Connected")


get_object()
start_websocket()

def getRiskFreeIntrRate() -> float:
        return (
            __import__("pandas")
            .json_normalize(
                __import__("requests")
                .get(
                    "https://misterdas.github.io"
                    + "/risk_free_interest_rate/RiskFreeInterestRate.json"
                )
                .json()
            )
            .query('GovernmentSecurityName == "91 day T-bills"')
            .reset_index()
            .Percent[0]
        )

try:
    ir = getRiskFreeIntrRate() / 100
    pass
except:
    print("IV Levels Not Synced")
    ir = 0.1
    pass

exchange = None
while True:
    if exchange is None:
        try:
            exchange = pd.DataFrame(kite.instruments())
            break
        except:
            print("Exchange Download Error...")
            time.sleep(10)

df = copy.deepcopy(exchange)
df = df[((df["exchange"] == "NFO") | (df["exchange"] == "BFO"))]
df = pd.DataFrame({"FNO Symbol": list(df["name"].unique())})
df = df[df["FNO Symbol"] != "SENSEX50"]
df = df.set_index("FNO Symbol", drop=True)
oc.range("a1").value = df

oc.range("d2").value, oc.range("d3").value, oc.range("d4").value, oc.range("d5").value = "Symbol==>>", "Option Expiry==>>", "Fut Expiry==>>", "Calc Base Fut==>>"
try:
    oc.range('d2:e50').api.Borders.Weight = 2
    oc.range('h1:bf250').api.Borders.Weight = 1
except:
    pass

pre_symbol = pre_oc_expiry = pre_fut_expiry = ""
oc_expiries_list = []
fut_expiries_list = []
instrument_dict = {}
prev_day_oi = {}
stop_thread = False


def get_oi(data):
    global prev_day_oi, kite, stop_thread
    for symbol, v in data.items():
        if stop_thread:
            break
        while True:
            try:
                prev_day_oi[symbol]
                break
            except:
                try:
                    pre_day_data = kite.historical_data(v["token"], (datetime.now() - timedelta(days=30)).date(),
                                          (datetime.now() - timedelta(days=1)).date(), "day", oi=True)
                    try:
                        prev_day_oi[symbol] = pre_day_data[-1]["oi"]
                    except:
                        prev_day_oi[symbol] = 0
                    break
                except Exception as e:
                    time.sleep(0.5)

#############

cur = datetime.now()
cur_date = cur.day
cur_month = cur.month
cur_year = cur.year

#############

# print("Excel : Started")

oc.range("d7:e330").value = oc.range("h:bf").value = None

while True:
    time.sleep(0.25)
    inp_symbol, inp_oc_expiry, inp_fut_expiry, inp_calc_base_fut, trigg = oc.range("e2").value, oc.range("e3").value, oc.range("e4").value, oc.range("e5").value, lc.range("b8").value
    if pre_symbol != inp_symbol or pre_oc_expiry != inp_oc_expiry or pre_fut_expiry != inp_fut_expiry:
        if token_symbol:
            kws.unsubscribe(list(token_symbol.keys()))
            time.sleep(2)
            tick_data = {}
            token_symbol = {}
        oc.range("g:bf").value = oc.range("d7:e33").value = None
        instrument_dict = {}
        stop_thread = True
        time.sleep(2)
        if pre_symbol != inp_symbol:
            oc.range("b:b").value = None
            oc_expiries_list = []
            fut_expiries_list = []
        pre_symbol = inp_symbol
        pre_oc_expiry = inp_oc_expiry
        pre_fut_expiry = inp_fut_expiry
    if inp_symbol is not None:
        try:
            if not oc_expiries_list:
                df = copy.deepcopy(exchange)
                df = df[((df["exchange"] == "NFO") | (df["exchange"] == "BFO"))]
                df = df[df["name"] == inp_symbol]
                df = df[(df["instrument_type"] == "CE") | (df["instrument_type"] == "PE")]
                oc_expiries_list = sorted(list(df["expiry"].unique()))
                df = pd.DataFrame({"Option Expiry Date": oc_expiries_list})
                df = df.set_index("Option Expiry Date", drop=True)
                oc.range("b20").value = df
            if not fut_expiries_list:
                df = copy.deepcopy(exchange)
                df = df[((df["exchange"] == "NFO") | (df["exchange"] == "BFO"))]
                df = df[df["name"] == inp_symbol]
                df = df[df["instrument_type"] == "FUT"]
                fut_expiries_list = sorted(list(df["expiry"].unique()))
                df = pd.DataFrame({"FUT Expiry Date": fut_expiries_list})
                df = df.set_index("FUT Expiry Date", drop=True)
                oc.range("b1").value = df
            if not instrument_dict and inp_oc_expiry is not None and inp_fut_expiry is not None:
                df = copy.deepcopy(exchange)
                df = df[((df["exchange"] == "NFO") | (df["exchange"] == "BFO"))]
                df = df[df["name"] == inp_symbol]
                df = df[(df["instrument_type"] == "CE") | (df["instrument_type"] == "PE")]
                df = df[df["expiry"] == inp_oc_expiry.date()]
                lot_size = list(df["lot_size"])[0]
                for i in df.index:
                    instrument_dict[f'{df["exchange"][i]}:{df["tradingsymbol"][i]}'] = {"strikePrice": float(df["strike"][i]),
                                                                        "instrumentType": df["instrument_type"][i],
                                                                        "token": df["instrument_token"][i]}
                    token_symbol[int(df["instrument_token"][i])] = f'{df["exchange"][i]}:{df["tradingsymbol"][i]}'
                df = copy.deepcopy(exchange)
                df = df[((df["exchange"] == "NFO") | (df["exchange"] == "BFO"))]
                df = df[df["name"] == inp_symbol]
                df = df[df["instrument_type"] == "FUT"]
                df = df[df["expiry"] == inp_fut_expiry.date()]
                for i in df.index:
                    fut_instrument = f'{df["exchange"][i]}:{df["tradingsymbol"][i]}'
                    instrument_dict[f'{df["exchange"][i]}:{df["tradingsymbol"][i]}'] = {"strikePrice": float(df["strike"][i]),
                                                                        "instrumentType": df["instrument_type"][i],
                                                                        "token": df["instrument_token"][i]}

                    token_symbol[int(df["instrument_token"][i])] = f'{df["exchange"][i]}:{df["tradingsymbol"][i]}'
                stop_thread = False
                thread = threading.Thread(target=get_oi, args=(instrument_dict,))
                thread.start()
            option_data = {}
            fut_data = {}
            spot_data = {}
            vix_data = {}
            index_map = {"NIFTY": "NSE:NIFTY 50", "BANKNIFTY": "NSE:NIFTY BANK", "FINNIFTY": "NSE:NIFTY FIN SERVICE",
                         "MIDCPNIFTY": "NSE:NIFTY MID SELECT", "SENSEX": "BSE:SENSEX", "BANKEX": "BSE:BANKEX",
                         "SENSEX50": "BSE:SENSEX50"}
            spot_instrument = index_map[inp_symbol] if inp_symbol in list(index_map) else f"NSE:{inp_symbol}"
            if not spot_instrument in list(token_symbol.values()):
                spot_df = copy.deepcopy(exchange)
                spot_token = list(spot_df[((spot_df["exchange"] == spot_instrument[:3]) & (spot_df["tradingsymbol"] == spot_instrument[4:]))]["instrument_token"])[0]
                token_symbol[int(spot_token)] = spot_instrument
                vix_df = copy.deepcopy(exchange)
                vix_token = list(
                    vix_df[((vix_df["exchange"] == "NSE") & (vix_df["tradingsymbol"] == "INDIA VIX"))][
                        "instrument_token"])[0]
                token_symbol[int(vix_token)] = "NSE:INDIA VIX"
                kws.subscribe(list(token_symbol.keys()))
                kws.set_mode(kws.MODE_FULL, list(token_symbol.keys()))
                time.sleep(2)

            for symbol, values in tick_data.copy().items():
                if symbol == spot_instrument:
                    spot_data = values
                elif symbol == "NSE:INDIA VIX":
                    vix_data = values
                elif symbol == fut_instrument:
                    fut_data = values

            for symbol, values in tick_data.copy().items():
                if symbol == spot_instrument or symbol == "NSE:INDIA VIX" or symbol == fut_instrument:
                    pass
                else:
                    try:
                        try:
                            option_data[symbol]
                        except:
                            option_data[symbol] = {}
                        option_data[symbol]["Strike_Price"] = instrument_dict[symbol]["strikePrice"]
                        option_data[symbol]["LOT"] = lot_size
                        option_data[symbol]["Instrument_Type"] = instrument_dict[symbol]["instrumentType"]
                        option_data[symbol]["LTP"] = values["last_price"]
                        option_data[symbol]["LTP_Change"] = values["last_price"] - values["ohlc"]["close"] if values["last_price"] != 0 else 0
                        option_data[symbol]["LTT"] = values["last_trade_time"]
                        option_data[symbol]["Total_Buy_Quantity"] = values["total_buy_quantity"]
                        option_data[symbol]["Total_Sell_Quantity"] = values["total_sell_quantity"]
                        option_data[symbol]["Average_Price"] = values["average_traded_price"]
                        option_data[symbol]["Open"] = values["ohlc"]["open"]
                        option_data[symbol]["High"] = values["ohlc"]["high"]
                        option_data[symbol]["Low"] = values["ohlc"]["low"]
                        option_data[symbol]["Best_Bid_Price"] = values["depth"]["buy"][0]["price"]
                        option_data[symbol]["Best_Ask_Price"] = values["depth"]["sell"][0]["price"]
                        option_data[symbol]["Prev_Close"] = values["ohlc"]["close"]
                        option_data[symbol]["Total_Traded_Volume"] = values["volume_traded"]
                        option_data[symbol]["OI"] = int(values["oi"]/lot_size)
                        try:
                            option_data[symbol]["OI_Change"] = int((values["oi"] - prev_day_oi[symbol])/lot_size)
                        except:
                            option_data[symbol]["OI_Change"] = None
                        if instrument_dict[symbol]["instrumentType"] == "CE":
                            option_data[symbol]["Intrinsic_Value" + ("(Fut)" if inp_calc_base_fut is True else "(Spot)")] = (fut_data["last_price"] if inp_calc_base_fut is True else spot_data["last_price"]) - instrument_dict[symbol]["strikePrice"]
                            option_data[symbol]["Time_Value" + ("(Fut)" if inp_calc_base_fut is True else "(Spot)")] = values["last_price"] - ((fut_data["last_price"] if inp_calc_base_fut is True else spot_data["last_price"]) - instrument_dict[symbol]["strikePrice"])
                        else:
                            option_data[symbol]["Intrinsic_Value" + ("(Fut)" if inp_calc_base_fut is True else "(Spot)")] = instrument_dict[symbol]["strikePrice"] - (fut_data["last_price"] if inp_calc_base_fut is True else spot_data["last_price"])
                            option_data[symbol]["Time_Value" + ("(Fut)" if inp_calc_base_fut is True else "(Spot)")] = values["last_price"] - (instrument_dict[symbol]["strikePrice"] - (fut_data["last_price"] if inp_calc_base_fut is True else spot_data["last_price"]))

                        

                        def greeks(premium, expiry, asset_price, strike_price, intrest_rate, instrument_type):
                            try:
                                if trigg == True:
                                    if lc.range("b7").value == True:
                                        year = int(lc.range("b4").value)
                                        month = int(lc.range("b5").value)
                                        day = int(lc.range("b6").value)
                                        t = ((datetime(expiry.year, expiry.month, expiry.day, 15, 30) - datetime(year, month, day, 15, 30)) / timedelta(days=1)) / 365
                                        premium = values["ohlc"]["close"]
                                        asset_price = (fut_data["ohlc"]["close"] if inp_calc_base_fut is True else spot_data["ohlc"]["close"])
                                    elif lc.range("b9").value == True:
                                        t = ((datetime(expiry.year, expiry.month, expiry.day, 15, 30) - datetime(int(cur_year), int(cur_month), int(cur_date), 9, 15)) / timedelta(days=1)) / 365
                                        premium = values["ohlc"]["open"]
                                        asset_price = (fut_data["ohlc"]["open"] if inp_calc_base_fut is True else spot_data["ohlc"]["open"])
                                    else:
                                        t = ((datetime(expiry.year, expiry.month, expiry.day, 15, 30) - datetime.now()) / timedelta(days=1)) / 365
                                else:
                                    t = ((datetime(expiry.year, expiry.month, expiry.day, 15, 30) - datetime.now()) / timedelta(days=1)) / 365
                                S = asset_price
                                K = strike_price
                                r = intrest_rate
                                if premium == 0 or t <= 0 or S <= 0 or K <= 0 or r <= 0:
                                    raise Exception
                                flag = instrument_type[0].lower()
                                imp_v = implied_volatility(premium, S, K, t, r, flag)
                                return {"IV": imp_v,
                                        "Delta": delta(flag, S, K, t, r, imp_v),
                                        "Gamma": gamma(flag, S, K, t, r, imp_v),
                                        "Rho": rho(flag, S, K, t, r, imp_v),
                                        "Theta": theta(flag, S, K, t, r, imp_v),
                                        "Vega": vega(flag, S, K, t, r, imp_v)}
                            except:
                                return {"IV": 0,
                                        "Delta": 0,
                                        "Gamma": 0,
                                        "Rho": 0,
                                        "Theta": 0,
                                        "Vega": 0}

                        greek = greeks(values["last_price"],
                                       inp_oc_expiry.date(),
                                       (fut_data["last_price"] if inp_calc_base_fut is True else spot_data["last_price"]),
                                       instrument_dict[symbol]["strikePrice"],
                                       ir,
                                       instrument_dict[symbol]["instrumentType"])
                        for k, v in greek.items():
                            option_data[symbol][k + ("(Fut)" if inp_calc_base_fut is True else "(Spot)")] = v
                    except Exception as e:
                        print(e)
                        pass

            df = pd.DataFrame(option_data).transpose()
            ce_df = df[df["Instrument_Type"] == "CE"]
            ce_df = ce_df.rename(columns={i: f"CE_{i}" for i in list(ce_df.keys())})
            ce_df.index = ce_df["CE_Strike_Price"]
            ce_df = ce_df.drop(["CE_Strike_Price"], axis=1)
            ce_df["Strike"] = ce_df.index

            pe_df = df[df["Instrument_Type"] == "PE"]
            pe_df = pe_df.rename(columns={i: f"PE_{i}" for i in list(pe_df.keys())})
            pe_df.index = pe_df["PE_Strike_Price"]
            pe_df = pe_df.drop("PE_Strike_Price", axis=1)
            df = pd.concat([ce_df, pe_df], axis=1).sort_index()
            df = df.replace(np.nan, 0)
            df["Strike"] = df.index
            total_profit_loss = {}
            for i in df.index:
                itm_call = df[df.index < i]
                itm_call_loss = (i - itm_call.index) * itm_call["CE_OI"]
                itm_put = df[df.index > i]
                itm_put_loss = (itm_put.index - i) * itm_put["PE_OI"]
                total_profit_loss[sum(itm_call_loss) + sum(itm_put_loss)] = i
            df.index = [np.nan] * len(df)
            try:
                fut_change_oi = fut_data["oi"] - prev_day_oi[fut_instrument]
            except:
                fut_change_oi = 0
            oc.range("d7").value = [["Spot LTP", spot_data["last_price"]],
                                    ["Spot Open", spot_data["ohlc"]["open"]],
                                    ["Spot High", spot_data["ohlc"]["high"]],
                                    ["Spot Low", spot_data["ohlc"]["low"]],
                                    ["Spot Prev Close", spot_data["ohlc"]["close"]],
                                    ["Spot LTP Change", spot_data["last_price"] - spot_data["ohlc"]["close"]],
                                    ["", ""],
                                    ["FUT LTP", fut_data["last_price"]],
                                    ["FUT Open", fut_data["ohlc"]["open"]],
                                    ["FUT High", fut_data["ohlc"]["high"]],
                                    ["FUT Low", fut_data["ohlc"]["low"]],
                                    ["FUT Prev Close", fut_data["ohlc"]["close"]],
                                    ["FUT LTP Change", fut_data["last_price"] - fut_data["ohlc"]["close"]],
                                    ["", ""],
                                    ["VIX LTP", vix_data["last_price"]],
                                    ["VIX Open", vix_data["ohlc"]["open"]],
                                    ["VIX High", vix_data["ohlc"]["high"]],
                                    ["VIX Low", vix_data["ohlc"]["low"]],
                                    ["VIX Prev Close", vix_data["ohlc"]["close"]],
                                    ["VIX LTP Change", vix_data["last_price"] - vix_data["ohlc"]["close"]],
                                    ["", ""],
                                    ["FUT OI", fut_data["oi"]],
                                    ["FUT Change in OI", fut_change_oi],
                                    ["", ""],
                                    ["Total Call OI", sum(list(df["CE_OI"]))],
                                    ["Total Put OI", sum(list(df["PE_OI"]))],
                                    ["Total Call Change in OI", sum(list(df["CE_OI_Change"]))],
                                    ["Total Put Change in OI", sum(list(df["PE_OI_Change"]))],
                                    ["", ""],
                                    ["Max Call OI", max(list(df["CE_OI"]))],
                                    ["Max Put OI", max(list(df["PE_OI"]))],
                                    ["Max Call OI Strike", list(df[df["CE_OI"] == max(list(df["CE_OI"]))]["Strike"])[0]],
                                    ["Max Put OI Strike", list(df[df["PE_OI"] == max(list(df["PE_OI"]))]["Strike"])[0]],
                                    ["",""],
                                    ["Max Call Change in OI", max(list(df["CE_OI_Change"]))],
                                    ["Max Put Change in OI", max(list(df["PE_OI_Change"]))],
                                    ["Max Call Change in OI Strike",
                                     list(df[df["CE_OI_Change"] == max(list(df["CE_OI_Change"]))]["Strike"])[0]],
                                    ["Max Put Change in OI Strike",
                                     list(df[df["PE_OI_Change"] == max(list(df["PE_OI_Change"]))]["Strike"])[0]],
                                    ["",""],
                                    ["PCR", round((sum(list(df["PE_OI"]))/sum(list(df["CE_OI"])) if sum(list(df["CE_OI"])) != 0 else 0), 2)],
                                    ["Max Pain Strike", total_profit_loss[min(list(total_profit_loss.keys()))]],
                                    ["Current Month", cur_month],
                                    ["Today's Date", cur_date],
                                    ["Current Year", cur_year]
                                    ]
            oc.range("g1").value = df  #[["CE_Total_Traded_Volume", "CE_LTP", "CE_LTP_Change", "CE_IV(Fut)", "CE_OI_Change", "CE_OI", "Strike", "PE_OI", "PE_OI_Change", "PE_IV(Fut)", "PE_LTP_Change", "PE_LTP", "PE_Total_Traded_Volume"]]
        except Exception as e:
            # print(e)
            pass
        