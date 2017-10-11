# -*- encoding: utf-8 -*-

import json


from quantos.util import fileio
from quantos.backtest import model
from quantos.backtest import common
from quantos.data.dataserver import JzDataServer
from quantos.example.doubleMaStrategy import DoubleMaStrategy
from quantos.backtest.backtest import EventBacktestInstance
from quantos.backtest.gateway import BarSimulatorGateway


def test_double_ma():
    prop_file_path = fileio.join_relative_path("etc/backtest.json")
    print prop_file_path
    prop_file = open(prop_file_path, 'r')
    
    props = json.load(prop_file)
    
    enum_props = {'bar_type': common.QUOTE_TYPE}
    for k, v in enum_props.iteritems():
        props[k] = v.to_enum(props[k])
    
    # strategy   = CtaStrategy()
    strategy = DoubleMaStrategy()
    gateway = BarSimulatorGateway()
    data_server = JzDataServer()

    context = model.Context()
    context.register_data_api(data_server)
    context.register_gateway(gateway)
    context.register_trade_api(gateway)
    
    backtest = EventBacktestInstance()
    backtest.init_from_config(props, strategy, context=context,
                              data_api=data_server, gateway=gateway, dataview=None)
    
    # backtest.run()
    backtest.run()
    report = backtest.generate_report()
    # print report.trades[:100]
    # for pnl in report.daily_pnls:
    #     print pnl.date, pnl.trade_pnl, pnl.hold_pnl,pnl.total_pnl, pnl.positions.get('600030.SH')


if __name__ == "__main__":
    test_double_ma()
