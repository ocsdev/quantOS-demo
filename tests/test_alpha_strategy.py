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

import numpy as np
import pandas as pd

from app.demoalphastrategy import AlphaStrategy, DemoAlphaStrategy
from data.dataserver import JzDataServer
from framework import model
from data.dataview import BaseDataView
from framework.alphabacktest import AlphaBacktestInstance
from framework.gateway import DailyStockSimGateway
import util.fileio


def read_props(fp):
    props = util.fileio.read_json(fp)
    
    enum_props = {}
    for k, v in enum_props.iteritems():
        props[k] = v.to_enum(props[k])
        
    return props


def my_selector(security, trade_date, dataview):
    df = dataview.get_snapshot(trade_date, security, 'close,pb')
    close = df.loc[security, 'close']
    pb = df.loc[security, 'pb']
    
    return close * pb > 123
    
    
def pb_factor(security, context=None, user_options=None):
    coef = user_options['coef']
    data_api = context.data_api
    # pb = data_api.get(security, field='pb', start_date=20170303, end_date=20170305)
    pb = 1.
    res = np.power(1. / pb, coef)
    return res


def my_commission(security, turnover, context=None, user_options=None):
    return turnover * user_options['myrate']


def test_alpha_strategy():
    gateway = DailyStockSimGateway()
    jz_data_server = JzDataServer()

    prop_file_path = r"../etc/alpha.json"
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
    context = model.Context()
    context.register_data_api(jz_data_server)
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
    backtest.init_from_config(props, jz_data_server, gateway, strategy)
    
    backtest.run_alpha()
    
    backtest.save_results('../output/')

    
"""
def main_new():
    import time
    t_start = time.time()
    
    prop_file_path = r"../etc/alpha.json"
    props = read_props(prop_file_path)
    
    data_server = JzDataServer()
    
    context = model.Context()
    context.register_data_api(data_server)

    revenue_model = model.FactorRevenueModel()
    revenue_model.register_context(context)
    revenue_model.register_func(pb_factor, 'pb_factor')
    revenue_model.activate_func({'pb_factor': {'mykwarg': 3.27}})
    
    cost_model = model.SimpleCostModel()
    cost_model.register_context(context)
    cost_model.activate_func({'default_commission': {'rate': 1e-3}})
    
    strategy = AlphaStrategy(None, None, cost_model)
    
    backtest = AlphaBacktestInstance()
    backtest.init_from_config(props, data_server, strategy)

    # strategy.register_selector(helper_function(conditions))
    
    opt_options = {"method": "mc", "params": {} }
    strategy.register_pc_method('optimizer', options)

    # options contains weights
    strategy.register_pc_method('self_defined_weight', options)
    # total market value or float market value
    strategy.register_pc_method('market_value_weight', options)
    
    strategy.register_pc_method('risk_parity', options)
    
    
    backtest.run_alpha()
    
    backtest.save_results('../output/')
    
    t3 = time.time() - t_start
    print "[backtest] time lapsed in total: {:.2f}".format(t3)

"""


if __name__ == "__main__":
    t_start = time.time()

    test_alpha_strategy()
    # test_prepare()
    # test_read()
    
    t3 = time.time() - t_start
    print "\n\n\nTime lapsed in total: {:.1f}".format(t3)

