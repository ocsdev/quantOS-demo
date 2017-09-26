
import app
from app import CtaStrategy
from app import *
import framework
from framework import *


props = {}
props['symbol'] = 'IF1709.CFE'
props['begin_date'] = 20170514
props['end_date'] = 20170520
props['folder'] = 'd:/'

props['jsh.addr'] = 'tcp://10.2.0.14:61616'
props['bar_type'] = QUOTE_TYPE_MINBAR
props['init_balance'] = 100000.0
props['future_commission_rate'] = 0.00002
props['stock_commission_rate'] = 0.0000
props['stock_tax_rate'] = 0.0000
instanceid = "demo"

#strategy   = CtaStrategy()
strategy   = DoubleMaStrategy()
gateway    = BarSimulatorGateway()
dataserver = JshHistoryBarDataServer()

backtest = BacktestInstance()
backtest.initFromConfig(instanceid, props, dataserver, gateway, strategy)

backtest.run()
report = backtest.generateReport()
#print report.trades[:100]
# for pnl in report.daily_pnls:
#     print pnl.date, pnl.trade_pnl, pnl.hold_pnl,pnl.total_pnl, pnl.positions.get('600030.SH')






