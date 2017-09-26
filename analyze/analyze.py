# encoding: utf-8

import numpy as np
import pandas as pd
import json
from framework.dataserver import JzDataServer
import matplotlib.pyplot as plt


class BaseAnalyzer(object):
    """
    Attributes
    ----------
    __trades : pd.DataFrame
    __configs : dict
    data_server : BaseDataServer
    __universe : set
        All securities that have been traded.
        
    """
    def __init__(self):
        self.__trades = None
        self.__configs = None
        self.data_server = None
        
        self.__universe = []
        self.__prices = None
        
    @property
    def trades(self):
        return self.__trades
    
    @property
    def universe(self):
        return self.__universe
    
    @property
    def configs(self):
        return self.__configs
    
    @property
    def prices(self):
        return self.__prices
    
    def initialize(self, data_server_, file_folder='../output/', ):
        """Read trades from csv file."""
        self.data_server = data_server_
        
        type_map = {'task_id': str,
                    'entrust_no': str,
                    'entrust_action': str,
                    'security': str,
                    'fill_price': float,
                    'fill_size': int,
                    'fill_date': int,
                    'fill_time': int,
                    'fill_no': str}
        trades = pd.read_csv(file_folder + 'trades.csv', ',', dtype=type_map)
        # for key, t in type_map.items():
        #     trades.loc[:, key] = trades.loc[:, key].values.astype(t)
        self.__trades = self._preprocess_trades(trades)
        
        configs = json.load(open(file_folder + 'configs.json', 'r'))
        self.__configs = configs
        
        self._init_universe()
        self._init_security_price()
    
    @staticmethod
    def _preprocess_trades(df):
        df.loc[:, 'fill_dt'] = df.loc[:, 'fill_date'] + df.loc[:, 'fill_time']
        return df
    
    def _init_security_price(self):
        df_dic = self.data_server.daily(','.join(self.universe), self.configs['start_date'], self.configs['end_date'], "CLOSE")
        # df_dic_named = [df.rename(columns={'CLOSE': sec}) for sec, df in df_dic.items()]
        df_list = []
        for sec, df in df_dic.items():
            df.index = pd.to_datetime(df.loc[:, 'DATE'], format="%Y%m%d")
            df.drop('DATE', axis=1, inplace=True)
            # df.rename(columns={'CLOSE': sec}, inplace=True)
            df.rename(columns={'CLOSE': 'close'}, inplace=True)
            df_list.append(df)
        df_all = pd.concat(df_list, axis=1)
        self.__prices = df_dic  # TODO
    
    def _init_universe(self):
        """Return a set of securities."""
        securities = self.trades.loc[:, 'security'].values
        self.__universe = set(securities)
    

class AlphaAnalyzer(BaseAnalyzer):
    @staticmethod
    def get_avg_pos_price(pos_arr, price_arr):
        assert len(pos_arr) == len(price_arr)
        
        avg_price = np.zeros_like(pos_arr, dtype=float)
        avg_price[0] = price_arr[0]
        for i in range(pos_arr.shape[0] - 1):
            if pos_arr[i+1] == 0:
                avg_price[i+1] = 0.0
            else:
                pos_diff = pos_arr[i+1] - pos_arr[i]
                if pos_arr[i] == 0 or (pos_diff) * pos_arr[i] > 0:
                    count = True
                else:
                    count = False
                
                if count:
                    avg_price[i+1] = (avg_price[i] * pos_arr[i] + pos_diff * price_arr[i+1]) * 1. / pos_arr[i+1]
                else:
                    avg_price[i+1] = avg_price[i]
        return avg_price
    
    @staticmethod
    def _process_trades(df):
        df = df.copy()
        
        cols_to_drop = ['task_id', 'entrust_no', 'fill_no']
        df.drop(cols_to_drop, axis=1, inplace=True)
        
        # df.loc[:, 'FillTime'] = pd.to_datetime(df.loc[:, 'FillTime'], format='%Y%m%d %H:%M:%S %f')
        
        df.loc[:, 'CumVolume'] = df.loc[:, 'fill_size'].cumsum()
        turnover = df.loc[:, 'fill_size'] * df.loc[:, 'fill_price']
        df.loc[:, 'CumTurnOver'] = turnover.cumsum()
        
        df.loc[:, 'direction'] = df.loc[:, 'entrust_action'].apply(lambda s: 1 if s == 'buy' else -1)
        df.loc[:, 'BuyVolume'] = (df.loc[:, 'direction'] + 1) / 2 * df.loc[:, 'fill_size']
        df.loc[:, 'SellVolume'] = (df.loc[:, 'direction'] - 1) / -2 * df.loc[:, 'fill_size']
        df.loc[:, 'CumNetTurnOver'] = (turnover * df.loc[:, 'direction'] * -1).cumsum()
        df.loc[:, 'position'] = (df.loc[:, 'fill_size'] * df.loc[:, 'direction']).cumsum()
        df.loc[:, 'AvgPosPrice'] = AlphaAnalyzer.get_avg_pos_price(df.loc[:, 'position'].values, df.loc[:, 'fill_price'].values)
        df.loc[:, 'VirtualProfit'] = (df.loc[:, 'CumNetTurnOver'] + df.loc[:, 'position'] * df.loc[:, 'fill_price'])
        # df.loc[:, 'commission'] = inst_param[inst]['commission'] * turnover
        # df.loc[:, 'VirtualProfit2'] = df.loc[:, 'VirtualProfit'] - df.loc[:, 'commission']
        
        # Execution related
        # df.loc[:, 'execution_luck'] = (df.loc[:, 'OrderPrice'] - df.loc[:, 'FillPrice']) * df.loc[:, 'direction']
        # df.loc[:, 'uncome_size'] = df.loc[:, 'OrderSize'] - df.loc[:, 'FillSize']
        
        return df
    
    def process_trades(self):
        trades = self.trades.copy()
        trades.loc[:, 'index'] = trades.index
        trades.index = pd.to_datetime(trades.loc[:, 'fill_date'], format="%Y%m%d")
    
        trades_dic = dict()
        for sec in self.universe:
            raw = trades.loc[trades.loc[:, 'security'] == sec, :]
            trades_dic[sec] = self._process_trades(raw)
    
        self.trades_dic = trades_dic
    
    def get_daily(self):
        daily_dic = dict()
        for sec, df in self.trades_dic.items():
            df_close = self.prices[sec]
            
            res = pd.concat([df_close, df], axis=1, join='outer')
            res = res.loc[:, ['close', 'BuyVolume', 'SellVolume', 'fill_size', 'position', 'AvgPosPrice', 'CumNetTurnOver']]
            
            cols_nan_to_zero = ['BuyVolume', 'SellVolume']
            cols_nan_fill = ['close', 'position', 'AvgPosPrice', 'CumNetTurnOver']
            res.loc[:, cols_nan_fill] = res.loc[:, cols_nan_fill].fillna(method='ffill')
            res.loc[:, cols_nan_fill] = res.loc[:, cols_nan_fill].fillna(0)
            res.loc[:, cols_nan_to_zero] = res.loc[:, cols_nan_to_zero].fillna(0)
            res.loc[res.loc[:, 'AvgPosPrice'] < 1e-5, 'AvgPosPrice'] = res.loc[:, 'close']
            
            res.loc[:, 'VirtualProfit'] = res.loc[:, 'CumNetTurnOver'] + res.loc[:, 'position'] * res.loc[:, 'close']
            
            daily_dic[sec] = res
        self.daily_dic = daily_dic
    
    def _to_return(self, arr):
        r = np.empty_like(arr)
        r[0] = 0.0
        r[1:] = arr[1:] / arr[0] - 1
        return r
    
    def get_total_pnl(self):
        l = [df.loc[:, 'VirtualProfit'].copy().rename({'VirtualProfit': sec}) for sec, df in self.daily_dic.items()]
        df = pd.concat(l, axis=1)
        pnl = df.sum(axis=1) * 100 + self.configs['init_balance']
        
        benchmark_name = '000300.SH'
        dic = self.data_server.daily(benchmark_name, self.configs['start_date'], self.configs['end_date'], "CLOSE")
        bench = dic[benchmark_name].drop('DATE', axis=1)

        pnl_return = pd.DataFrame(index=pnl.index, data=self._to_return(pnl.values))
        bench_return = pd.DataFrame(index=bench.index, data=self._to_return(bench.values))
        
        df = pd.concat([bench_return, pnl_return], axis=1).fillna(method='ffill')
        df.columns = [benchmark_name, 'Strategy']
        df.loc[:, 'extra'] = df.loc[:, 'Strategy'] - df.loc[:, benchmark_name]
        start = pd.to_datetime(self.configs['start_date'], format="%Y%d%m")
        end = pd.to_datetime(self.configs['end_date'], format="%Y%d%m")
        years = (end - start).days / 225.
        
        yearly_return = df.loc[:, 'extra'].values[-1] / years
        yearly_vol = df.loc[:, 'extra'].std() / np.sqrt(225.)
        beta = np.corrcoef(df.loc[:, benchmark_name], df.loc[:, 'Strategy'])[0, 1]
        sharpe = yearly_return / yearly_vol

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8))
        ax1.plot(df.loc[:, benchmark_name], label='Benchmark')
        ax1.plot(df.loc[:, 'Strategy'], label='Strategy')
        ax1.legend()
        ax2.plot(df.loc[:, 'extra'])
        ax2.set_title("Extra Return, beta={:.2f}, yearly_return={:.2f}, yearly_vol={:.2f}, sharpe={:.2f}".format(beta, yearly_return, yearly_vol, sharpe))
        plt.show()


def calc_uat_metrics(t1, security):
    cump1 = t1.loc[:, 'VirtualProfit'].values
    profit1 = cump1[-1]
    
    n_trades = t1.loc[:, 'CumVolume'].values[-1] / 2.  # signle
    avg_trade = profit1 / n_trades
    print "profit without commission = {} \nprofit with commission {}".format(profit1, profit1)
    print "avg_trade = {:.3f}".format(avg_trade)
    
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(16, 8))
    ax1.plot(cump1, label='inst1')
    ax1.set_title("{} PnL in price".format(security))
    ax1.legend(loc='upper left')
    ax1.axhline(0, color='k', lw=1, ls='--')
    ax2.plot(t1.loc[:, 'position'].values)
    ax2.set_title("Position")
    ax2.axhline(0, color='k', lw=1, ls='--')
    
    plt.show()
    return


def plot_trades(df, security=""):
    idx = range(len(df.index))
    price = df.loc[:, 'close']
    bv, sv = df.loc[:, 'BuyVolume'].values, df.loc[:, 'SellVolume'].values
    profit = df.loc[:, 'VirtualProfit'].values
    avgpx = df.loc[:, 'AvgPosPrice']
    bv *= .3
    sv *= .3
    
    fig = plt.figure(figsize=(14, 10))
    ax1 = plt.subplot2grid((4, 1), (0, 0), rowspan=3)
    ax3 = plt.subplot2grid((4, 1), (3, 0), rowspan=1, sharex=ax1)
    
    # fig, (ax1, ax3) = plt.subplots(2, 1, figsize=(16, 18), sharex=True)
    # fig, ax1 = plt.subplots(1, 1, figsize=(16, 6))
    ax2 = ax1.twinx()
    
    ax1.plot(idx, price, label='Price', linestyle='-', lw=1, marker='', color='yellow')
    ax1.scatter(idx, price, label='buy', marker='o', s=bv, color='red')
    ax1.scatter(idx, price, label='sell', marker='o', s=sv, color='green')
    ax1.plot(idx, avgpx, lw=1, marker='', color='green')
    ax1.legend(loc='upper left')
    
    ax2.plot(idx, profit, label='PnL', color='k', lw=1, ls='--', alpha=.4)
    ax2.legend(loc='upper right')
    
    # ax1.xaxis.set_major_formatter(MyFormatter(df.index))#, '%H:%M'))
    
    ax3.plot(idx, df.loc[:, 'position'], marker='D', markersize=3, lw=2)
    ax3.axhline(0, color='k', lw=1)
    
    ax1.set_title(security)
    # plt.tight_layout()
    return

if __name__ == "__main__":
    ta = AlphaAnalyzer()
    data_server = JzDataServer()
    
    ta.initialize(data_server, '../output/test/')
    ta.process_trades()
    ta.get_daily()

    assert len(ta.universe) == 6
    # assert ta.prices.shape == (344, 6)
    
    for sec, df in ta.daily_dic.items():
        plot_trades(df, sec)
        plt.show()

    ta.get_total_pnl()
    # for sec, df in ta.trades_dic.items():
    #     calc_uat_metrics(df, sec)
    
    print "Test passed."
