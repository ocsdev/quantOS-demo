# encoding: utf-8
import quantos.backtest.analyze.analyze as ana
from quantos.data.dataserver import JzDataServer
from quantos import SOURCE_ROOT_DIR
import os


def test_backtest_analyze():
    ta = ana.AlphaAnalyzer()
    data_server = JzDataServer()
    
    ta.initialize(data_server, '../output/')
    
    print "process trades..."
    ta.process_trades()
    print "get daily stats..."
    ta.get_daily()
    print "calc strategy return..."
    ta.get_returns()
    print "get position change..."
    ta.get_pos_change_info()
    
    out_foler = "../output"

    print "plot..."
    selected_sec = list(ta.universe)[::3]
    for sec, df in ta.daily.items():
        if sec in selected_sec:
            ana.plot_trades(df, sec, out_foler)
    ta.plot_pnl(out_foler)
    print "generate report..."
    
    ta.gen_report(out_foler)


if __name__ == "__main__":
    import time
    t_start = time.time()

    test_backtest_analyze()
    
    t1 = time.time() - t_start
    print "\ntime lapsed in total: {:.2f}".format(t1)
