#!/usr/bin/env python
# import logging
from binance.um_futures import UMFutures
import time
import datetime as dt
import pytz
import tti.indicators as ti
import pandas as pd
from pandas_datareader import data as pdr
import numpy as np

# Starting the client
client = UMFutures()

# get server time
print(client.time())

# Constantes
api_key_f = ''
api_secret_f = ''

contractype = 'PERPETUAL'

# Variables
PSL_L = 0
PSL_S = 0
OP_L = False
OP_S = False
REF = 'USDT'
CRIPTO = 'LINK'
CAPITAL = 2  # Capital a invertir por operación
POSITION_SIDE = 'LONG'
PAR = CRIPTO + REF
contractType = 'PERPETUAL'
Temporalidad = 15  # tiempo en minutos para las velas
Price = 0.00000  # Precio del ticket

FBalance = 0.00000
minQty = 0.00000
stepSize = 0.00000
maxQty = 0.00000
LEVERAGE = 5
limit = 26 + 1
SIDE = 'LONG'
sma_f = 9
sma_s = 26


class Cripto_Bot():

    def __init__(self, api_key, api_secret, cripto, ref, period, leverage, contractType, capital, side, sma_f, sma_s):
        self.client = UMFutures(key=api_key, secret=api_secret)
        #
        try:
            self.client.change_position_mode(dualSidePosition=True)
        except:
            print('error changin position mode')
            pass

        # set parametros
        self.cripto = cripto
        self.ref = ref
        self.exchange = self.cripto + self.ref
        self.side = side
        self.contractType = contractType
        self.market_price = 0.0
        self.single_operation_capital = capital
        self.leverage = leverage
        self.SMA_F = sma_f
        self.SMA_S = sma_s
        self.period = period
        self.pe_l = 0.0  # Precio de entrada en LONG
        self.pe_s = 0.0  # Precio de entrada en SHORT
        self.dsl_l = 0.0  # StopLoss dinámico para LONG
        self.dsl_s = 0.0  # StopLoss dinámico para SHORT
        self.OP_L = False  # Operacion abierta en LONG
        self.OP_S = False  # Operacion abierta en SHORT
        self.crossup = False  # EMA rapida cruza sobre la EMA lenta
        self.crossdown = False  # EMA rapida cruza por debajo la EMA lenta
        self.sl00_l = False
        self.sl0_l = False
        self.sl1_l = False
        self.sl2_l = False
        self.sl3_l = False
        self.sl4_l = False
        self.sl5_l = False
        self.sl6_l = False
        self.sl7_l = False
        self.sl8_l = False
        self.sl00_s = False
        self.sl0_s = False
        self.sl1_s = False
        self.sl2_s = False
        self.sl3_s = False
        self.sl4_s = False
        self.sl5_s = False
        self.sl6_s = False
        self.sl7_s = False
        self.sl8_s = False
        # self.dfres = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'start', 'SMA_F', 'SMA_S'])

        try:
            chng_leverage = self.client.change_leverage(PAR, LEVERAGE, recvWindow=6000)
            # chng_leverage=self.client.change_leverage(par,LEVERAGE, recvWindow=6000)#change_leverage(symbol=self.exchange, leverage=self.leverage)
            print('leverage has changed')
        except:
            self.RUN = False
            print('Error leverage')

            # Filtros

        result = self.client.exchange_info()
        self.minQty, self.stepSize, self.maxQty = Get_Exchange_filters(result, self.exchange)
        self.maxDeciamlQty = Calculate_max_Decimal_Qty(self.stepSize)
        self.capital = Get_Capital(self.client.account(recvWindow=6000), self.ref)

            # Variables logisticas

        self.df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'start', 'SMA_F', 'SMA_S'])

        self.buysignal = None
        self.sellsignal = None
        self.quantity = None
        self.quantity_l = None
        self.quantity_s = None
        self.open = False

        self.RUN = True

    def Order(self, side, price,type):
        Qty = Calculate_Qty(price, self.single_operation_capital * self.leverage, self.minQty, self.maxQty,
                            self.maxDeciamlQty)
        if not Qty:
            self.RUN = False
        if type == 'LONG':
            if side == 'BUY':
                self.quantity_l = Qty
                self.OP_L = True
            else:
                self.OP_L = False
        if type == 'SHORT':
            if side == 'SELL':
                self.quantity_s = Qty
                self.OP_S = True
            else:
                self.OP_S = False

        response = self.client.new_order(symbol=self.exchange, side=side, type='MARKET', quantity=self.quantity,positionSide=self.side)
        dfres = pd.DataFrame(response)
        dfres.to_csv("MyTrades.csv")
    def Last_data(self):

        if self.df.shape[0] == 0:
            # candles = self.client.get_candlestick_data(symbol=self.exchange, interval=self.period,limit=self.SMA_S + 1)
            candles = self.client.continuous_klines(pair=self.exchange, contractType=contractType, interval=self.period,
                                                    limit=27)
            df_temp = Parse_data1(candles, limit=27)
            self.df = self.df._append(df_temp, ignore_index=True)
            print(self.df)


        else:

            candles = self.client.continuous_klines(pair=self.exchange, contractType=contractType, interval=self.period,
                                                    limit=1)
            df_temp = Parse_data1(candles, limit=1)
            self.df = self.df._append(df_temp, ignore_index=True)
            self.df = self.df.drop(index=0)
            self.df.index = list(range(27))
            smas = sma(self.df.close, 26, 'exponential')
            smaf = sma(self.df.close, 9, 'exponential')
            ma_f = smaf[0][0]
            ma_s = smas[0][0]
            self.df.SMA_F[26] = ma_f
            self.df.SMA_S[26] = ma_s
        # Definir Cruce de EMA como señal de logn o short
        self.crossup = Crossover(self.df.SMA_F.values[-2:], self.df.SMA_S.values[-2:])
        self.crossdown = Crossover(self.df.SMA_S.values[-2:], self.df.SMA_F.values[-2:])

        # Estrategia
        if self.side == 'LONG':
            self.buysignal = Crossover(self.df.SMA_F.values[-2:], self.df.SMA_S.values[-2:])
            self.sellsignal = Crossover(self.df.SMA_S.values[-2:], self.df.SMA_F.values[-2:])

        else:

            self.buysignal = Crossover(self.df.SMA_S.values[-2:], self.df.SMA_F.values[-2:])
            self.sellsignal = Crossover(self.df.SMA_F.values[-2:], self.df.SMA_S.values[-2:])

    def dsll(self, price):
        # Calculo del StopLoss Dinámico para LONG
        if (price > (self.pe_l / 100) * 1.5 and price < (self.pe_l / 100) * 2.5) and self.sl00_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 1.2)
            self.sl00_l = True
        if (price > (self.pe_l / 100) * 2.5 and price < (self.pe_l / 100) * 3) and self.sl0_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 2.2)
            self.sl0_l = True
        if (price > (self.pe_l / 100) * 3 and price < (self.pe_l / 100) * 4) and self.sl1_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 2.5)
            self.sl1_l = True
        if (price > (self.pe_l / 100) * 4 and price < (self.pe_l / 100) * 5) and self.sl2_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 3.5)
            self.sl2_l = True
        if (price > (self.pe_l / 100) * 5 and price < (self.pe_l / 100) * 6) and self.sl3_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 4.5)
            self.sl3_l = True
        if (price > (self.pe_l / 100) * 6 and price < (self.pe_l / 100) * 7) and self.sl4_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 6.5)
            self.sl4_l = True
        if (price > (self.pe_l / 100) * 7 and price < (self.pe_l / 100) * 9) and self.sl5_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 6.5)
            self.sl5_l = True
        if (price > (self.pe_l / 100) * 9 and price < (self.pe_l / 100) * 11) and self.sl6_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 8.8)
            self.sl6_l = True
        if (price > (self.pe_l / 100) * 11 and price < (self.pe_l / 100) * 13) and self.sl7_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 10.8)
            self.sl7_l = True
        if (price > (self.pe_l / 100) * 13) and self.sl8_l == False:
            self.dst_l = (self.pe_l + (self.pe_l / 100) * 12.8)
            self.sl8_l = True
        return self.dst_l

    def dsls(self, price):
        # Calculo del StopLoss Dinámico para SHORT
        if (price > (self.pe_s / 100) * 1.5 and price < (self.pe_s / 100) * 2.5) and self.sl00_s == False:
            self.dst_s = (price - (self.pe_s / 100) * 1.2)
            self.sl00_s = True
        if (price > (self.pe_s / 100) * 2.5 and price < (self.pe_s / 100) * 3) and self.sl0_s == False:
            self.dst_s = (price - (self.pe_s / 100) * 2.2)
            self.sl0_s = True
        if (price > (self.pe_s / 100) * 3 and price < (self.pe_s / 100) * 4) and self.sl1_s == False:
            self.dst_s = (price - (self.pe_s / 100) * 2.5)
            self.sl1_s = True
        if (price > (self.pe_s / 100) * 4 and price < (self.pe_s / 100) * 5) and self.sl2_s == False:
            self.dst_s = (price - (self.pe_s / 100) * 3.5)
            self.sl2_s = True
        if (price > (self.pe_s / 100) * 5 and price < (self.pe_s / 100) * 6) and self.sl3_s == False:
            self.dst_s = (self.pe_s + (self.pe_s / 100) * 4.5)
            self.sl3_s = True
        if (price > (self.pe_s / 100) * 6 and price < (self.pe_s / 100) * 7) and self.sl4_s == False:
            self.dst_s = (self.pe_s + (self.pe_s / 100) * 5.5)
            self.sl4_s = True
        if (price > (self.pe_s / 100) * 7 and price < (self.pe_s / 100) * 9) and self.sl5_s == False:
            self.dst_s = (self.pe_s + (self.pe_s / 100) * 6.5)
            self.sl5_s = True
        if (price > (self.pe_s / 100) * 9 and price < (self.pe_s / 100) * 11) and self.sl6_s == False:
            self.dst_s = (self.pe_s + (self.pe_s / 100) * 8.8)
            self.sl6_s = True
        if (price > (self.pe_s / 100) * 11 and price < (self.pe_s / 100) * 13) and self.sl7_s == False:
            self.dst_s = (self.pe_s + (self.pe_s / 100) * 10.8)
            self.sl7_s = True
        if (price > (self.pe_s / 100) * 13) and self.sl8_s == False:
            self.dst_s = (self.pe_s + (self.pe_s / 100) * 12.8)
            self.sl8_s = True
        return self.dst_s

    def Single_Operation(self):
        self.capital = Get_Capital(self.client.account(), self.ref)
        if float(self.capital) <= self.single_operation_capital:
            print('Dinero no suficiente')
            self.RUN = False
        # actualizar datos
        self.Last_data()

        # precio actual
        price = float(self.client.ticker_price(symbol=self.exchange)['price'])
        PSL_L = self.dsll(price)
        PSL_S = self.dsls(price)

        print(price)

        if self.OP_L:
            if price <= PSL_L:
                    side = 'SELL'
                    try:
                        self.Order(side=side, price=price, type='LONG')
                        print("Vendio")
                        self.sl00_l = False
                        self.sl0_l = False
                        self.sl1_l = False
                        self.sl2_l = False
                        self.sl3_l = False
                        self.sl4_l = False
                        self.sl5_l = False
                        self.sl6_l = False
                        self.sl7_l = False
                        self.sl8_l = False
                    except Exception as e:
                        print(e)
        if self.OP_S:
            if price >= PSL_S:

                side = 'BUY'
                try:
                    self.Order(side=side, price=price)
                    print("Compro a " + str(price))
                    self.sl00_s = False
                    self.sl0_s = False
                    self.sl1_s = False
                    self.sl2_s = False
                    self.sl3_s = False
                    self.sl4_s = False
                    self.sl5_s = False
                    self.sl6_s = False
                    self.sl7_s = False
                    self.sl8_s = False

                    # self.H_df.operacion.iloc[-1] = 'BUY'
                except Exception as e:
                    print(e)

        if self.crossup:

                    side = 'BUY'
                    try:
                        self.Order(side=side, price=price)
                        self.pe_l = float(self.client.ticker_price(symbol=self.exchange)['price'])
                        self.OP_L = True
                    except Exception as e:
                        print(e)
        if self.crossdown:
                    side = 'SELL'
                    try:
                        self.Order(side=side, price=price)
                        self.pe_s = float(self.client.ticker_price(symbol=self.exchange)['price'])
                        self.OP_S = True
                    except Exception as e:
                        print(e)

    def run(self):
        if 'm' in self.period:
            if len(self.period) == 2:
                step = int(self.period[0])
            else:
                step = int(self.period[:2])
        elif self.period == '15m':
            step = 15
        else:
            print('interval error')
            return
        self.Last_data()
        START = self.df.start.iloc[-1] + dt.timedelta(minutes=step)
        print(START)
        while dt.datetime.now(dt.timezone.utc) < pytz.UTC.localize(START):
            time.sleep(1)
            pass
            print('Strarting Bot...\n')
        time.sleep(3)  # para ser seguros de encontrar los datos de la velas siguente
        print('Bot started')
        while self.RUN:
            temp = time.time()
            self.Single_Operation()
            retraso = time.time() - temp

            print("Waiting next Candle..." + str((60 * step - retraso) / 60))

            time.sleep(60 * step - retraso)


def Get_Capital(data, ref):
    # Ac_info = um_futures_client.account(recvWindow=6000)
    for i in range(len(data['assets'])):
        if data['assets'][i]['asset'] == ref:
            Ainfo = data['assets'][i]['marginBalance']
            # print("Margin Balance: "+str(Ainfo))
    return Ainfo


def Get_Exchange_filters(result, par):
    for i in range(len(result['symbols'])):
        if result['symbols'][i]['symbol'] == par:
            minQty = float(result['symbols'][i]['filters'][2]['minQty'])
            stepSize = float(result['symbols'][i]['filters'][2]['stepSize'])
            maxQty = float(result['symbols'][i]['filters'][2]['maxQty'])

            return minQty, stepSize, maxQty


def Calculate_max_Decimal_Qty(stepSize):
    max_decimal_quantity = 0
    a = 10
    while stepSize * a < 1:
        a = a * 10 ** max_decimal_quantity
        max_decimal_quantity += 1
    return max_decimal_quantity


def Calculate_Qty(price, money, minQty, maxQty, maxDeciamlQty):
    Q = money / price
    if (Q < minQty or Q > maxQty):
        return False
    Q = np.round(Q, maxDeciamlQty)
    return Q


def sma(data, sma_period, ma_type):
    # df = df.assign(EMA_9=ema_9)
    data_h = get_dataKL(data)
    dfma = pd.DataFrame(data_h.close)
    ma = ti.MovingAverage(input_data=dfma, period=sma_period, ma_type=ma_type)
    return [ma.getTiValue(), ma.getTiData()]


def get_dataKL(cnd):
    df = pd.DataFrame(cnd, columns=['Open Time', 'Open', 'High', 'Low', 'close', 'Volume', 'Close Time',
                                    'Quote asset volume', 'Number of Trades', 'Taker Buy Base Asset Volume',
                                    'Taker Buy Quote Asset Volume', 'Nothing'])
    # Dar formato de fecha en milisegundos a las columnas "Open Time" y "Close Time"
    df['Datetime'] = pd.DatetimeIndex(pd.to_datetime(df['Close Time'], unit='ms'))
    df['Date'] = df['Open Time'].apply(pd.to_numeric)
    df['Close Time'] = pd.to_datetime(df['Close Time'], unit='ms')

    # Mantener las demás columnas como números flotantes
    columnas_numericas = ['Open', 'High', 'Low', 'close', 'Volume', 'Quote asset volume', 'Taker Buy Base Asset Volume',
                          'Taker Buy Quote Asset Volume']
    df[columnas_numericas] = df[columnas_numericas].astype(float)
    df = df.set_index('Datetime')

    # Eliminar advertencias de fillna

    return df


def Parse_data1(result, limit):
    """
    :param result:
    :param limit:
    :return:
    """
    data = []
    for i in range(limit):
        vela = []
        vela.append(result[i][0])
        vela.append(result[i][1])
        vela.append(result[i][2])
        vela.append(result[i][3])
        vela.append(result[i][4])
        vela.append(result[i][5])
        data.append(vela)
    df1 = pd.DataFrame(data)
    col_names = ['time', 'open', 'high', 'low', 'close', 'volume']
    df1.columns = col_names
    for col in col_names:
        df1[col] = df1[col].astype(float)

    df1['start'] = pd.to_datetime(df1['time'] * 1000000)

    return df1


def Crossover(MF, MS):
    if (MF[0] < MS[0] and MF[1] >= MS[1]):
        return True
    else:
        return False


# Inicializar el bot
Bot = Cripto_Bot(api_key=api_key_f, api_secret=api_secret_f, cripto=CRIPTO, ref=REF, contractType=contractType,
                 period=str(Temporalidad) + 'm', leverage=LEVERAGE, capital=CAPITAL, side=SIDE, sma_f=sma_f,
                 sma_s=sma_s)

Bot.run()
