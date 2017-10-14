# encoding: utf-8

from quantos.util import fileio
import quantos.backtest.analyze.analyze as ana
from quantos.data.dataservice import RemoteDataService


def test_backtest_analyze():
    ta = ana.AlphaAnalyzer()
    data_service = RemoteDataService()

    out_folder = fileio.join_relative_path("../output")

    ta.initialize(data_service, out_folder)
    
    print "process trades..."
    ta.process_trades()
    print "get daily stats..."
    ta.get_daily()
    print "calc strategy return..."
    ta.get_returns()
    # print "get position change..."
    # ta.get_pos_change_info()
    
    selected_sec = []  # list(ta.universe)[:3]
    if len(selected_sec) > 0:
        print "Plot single securities PnL"
        for symbol in selected_sec:
            df_daily = ta.daily.get(symbol, None)
            if df_daily is not None:
                ana.plot_trades(df_daily, symbol=symbol, save_folder=out_folder)

    print "Plot strategy PnL..."
    ta.plot_pnl(out_folder)
    
    print "generate report..."
    static_folder = fileio.join_relative_path("backtest/analyze/static")
    ta.gen_report(source_dir=static_folder, template_fn='report_template.html',
                  css_fn='blueprint.css', out_folder=out_folder,
                  selected=selected_sec)


if __name__ == "__main__":
    import time
    t_start = time.time()

    test_backtest_analyze()
    
    t1 = time.time() - t_start
    print "\ntime lapsed in total: {:.2f}".format(t1)
