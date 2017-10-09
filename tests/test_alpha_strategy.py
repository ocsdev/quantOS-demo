# -*- encoding: utf-8 -*-

"""
1. filter universe: separate helper functions
2. calc weights
3. generate trades

------------------------

- modify models: register function (with context parameter)
- modify AlphaStrategy: inheritate

------------------------

suspensions and limit reachers:
1. deal with them in re_balance function, not in filter_universe
2. do not care about them when construct portfolio
3. subtract market value and re-normalize weights (positions) after (daily) market open, before sending orders
"""
import time
import os

import numpy as np
from quantos.data.dataserver import JzDataServer
from quantos.example.demoalphastrategy import DemoAlphaStrategy

import quantos.util.fileio
from quantos.backtest.backtest import AlphaBacktestInstance, AlphaBacktestInstance2
from quantos.backtest.gateway import DailyStockSimGateway
from quantos.backtest import model
from quantos.data.dataview import BaseDataView


def read_props(fp):
    props = quantos.util.fileio.read_json(fp)
    
    enum_props = {}
    for k, v in enum_props.iteritems():
        props[k] = v.to_enum(props[k])
        
    return props


def my_selector(symbol, trade_date, dataview):
    df = dataview.get_snapshot(trade_date, symbol, 'close,pb')
    close = df.loc[symbol, 'close']
    pb = df.loc[symbol, 'pb']
    
    return close * pb > 123
    
    
def pb_factor(symbol, context=None, user_options=None):
    coef = user_options['coef']
    data_api = context.data_api
    # pb = data_api.get(symbol, field='pb', start_date=20170303, end_date=20170305)
    pb = 1.
    res = np.power(1. / pb, coef)
    return res


def pb_factor(symbol, context=None, user_options=None):
    dv = context.dataview
    return dv.get_snapshot(fields="fpb")

def my_commission(symbol, turnover, context=None, user_options=None):
    return turnover * user_options['myrate']


def test_alpha_strategy():
    gateway = DailyStockSimGateway()
    jz_data_server = JzDataServer()

    prop_file_path = os.path.join(os.path.dirname(__file__),
                                  '../quantos/etc', 'alpha.json')
    props = read_props(prop_file_path)
    """
    props = {
        "benchmark": "000300.SH",
        "universe": "600026.SH,600027.SH,600028.SH,600029.SH,600030.SH,600031.SH",
    
        "period": "month",
        "days_delay": 1,
    
        "init_balance": 1e7,
        "position_ratio": 0.7,
    
        "start_date": 20120101,
        "end_date": 20170601,
    
        "instanceid": "alpha001"
        }

    """

    jz_data_server.init_from_config(props)
    jz_data_server.initialize()
    gateway.init_from_config(props)

    context = model.Context()
    context.register_data_api(jz_data_server)
    context.register_gateway(gateway)
    context.register_trade_api(gateway)
    
    risk_model = model.FactorRiskModel()
    signal_model = model.FactorRevenueModel()
    cost_model = model.SimpleCostModel()
    
    risk_model.register_context(context)
    signal_model.register_context(context)
    cost_model.register_context(context)
    
    signal_model.register_func(pb_factor, 'pb_factor')
    signal_model.activate_func({'pb_factor': {'coef': 3.27}})
    cost_model.register_func(my_commission, 'my_commission')
    cost_model.activate_func({'my_commission': {'myrate': 1e-2}})
    
    strategy = DemoAlphaStrategy(risk_model, signal_model, cost_model)
    # strategy.register_context(context)
    # strategy.active_pc_method = 'equal_weight'
    strategy.active_pc_method = 'mc'
    
    backtest = AlphaBacktestInstance()
    backtest.init_from_config(props, strategy, context=context)
    
    backtest.run_alpha()
    
    backtest.save_results('../output/')
    
    
def save_dataview():
    from quantos.data.dataserver import JzDataServer
    
    ds = JzDataServer()
    dv = BaseDataView()
    
    props = {'start_date': 20141114, 'end_date': 20161114, 'universe': '000300.SH',
             'fields': 'open,close,high,low,volume,turnover,vwap,' + 'oper_rev,oper_exp',
             'freq': 1}
    
    dv.init_from_config(props, data_api=ds)
    dv.prepare_data()
    dv.save_dataview(folder_path='../output/prepared')


def test_alpha_strategy_dataview():
    dv = BaseDataView()
    folder_path = '../output/prepared/20141114_20161114_freq=1D'
    dv.load_dataview(folder=folder_path)
    
    props = {
        "benchmark": "000300.SH",
        # "symbol": ','.join(dv.symbol),
        "universe": ','.join(dv.symbol),
    
        "start_date": dv.start_date,
        "end_date": dv.end_date,
    
        "instanceid": "alpha001",
        
        "period": "month",
        "days_delay": 1,
    
        "init_balance": 1e7,
        "position_ratio": 0.7,
        }

    gateway = DailyStockSimGateway()
    gateway.init_from_config(props)

    context = model.Context()
    context.register_gateway(gateway)
    context.register_trade_api(gateway)
    context.register_dataview(dv)
    
    risk_model = model.FactorRiskModel()
    signal_model = model.FactorRevenueModel()
    cost_model = model.SimpleCostModel()
    
    risk_model.register_context(context)
    signal_model.register_context(context)
    cost_model.register_context(context)
    
    # signal_model.register_func(pb_factor, 'pb_factor')
    signal_model.register_func('pb_factor')
    signal_model.activate_func({'pb_factor': {'coef': 3.27}})
    cost_model.register_func(my_commission, 'my_commission')
    cost_model.activate_func({'my_commission': {'myrate': 1e-2}})
    
    strategy = DemoAlphaStrategy(risk_model, signal_model, cost_model)
    # strategy.register_context(context)
    # strategy.active_pc_method = 'equal_weight'
    strategy.active_pc_method = 'mc'
    
    backtest = AlphaBacktestInstance2()
    backtest.init_from_config(props, strategy, context=context)
    
    backtest.run_alpha()
    
    backtest.save_results('../output/')

if __name__ == "__main__":
    t_start = time.time()

    # save_dataview()
    # test_alpha_strategy()
    test_alpha_strategy_dataview()
    # test_prepare()
    # test_read()
    
    t3 = time.time() - t_start
    print "\n\n\nTime lapsed in total: {:.1f}".format(t3)

