# encoding: utf-8

from quantos.util import fileio
import quantos.backtest.analyze.analyze as ana
from quantos.data.dataservice import RemoteDataService
from quantos import SOURCE_ROOT_DIR


def test_backtest_analyze():
    ta = ana.AlphaAnalyzer()
    data_service = RemoteDataService()

    out_folder = fileio.join_relative_path("../output")
    static_folder = fileio.join_relative_path("backtest/analyze/static")

    ta.initialize(data_service, '../output/')
    
    print "process trades..."
    ta.process_trades()
    print "get daily stats..."
    ta.get_daily()
    print "calc strategy return..."
    ta.get_returns()
    print "get position change..."
    # ta.get_pos_change_info()
    
    print "plot..."
    selected_sec = []  # list(ta.universe)[::3]
    for sec, df in ta.daily.items():
        if sec in selected_sec:
            ana.plot_trades(df, sec, out_folder)
    ta.plot_pnl(out_folder)
    print "generate report..."
    
    ta.gen_report(static_folder=static_folder, out_folder=out_folder, selected=selected_sec)


if __name__ == "__main__":
    import time
    t_start = time.time()

    test_backtest_analyze()
    
    t1 = time.time() - t_start
    print "\ntime lapsed in total: {:.2f}".format(t1)
