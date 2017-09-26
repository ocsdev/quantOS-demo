# encoding: UTF-8

import pytest
from data.dataserver import JzDataServer
from data.py_expression_eval import Parser
import pandas as pd


def test_skew():
    expression = parser.parse('Ts_Skewness(open,4)')
    print expression.evaluate({'close': dfy, 'open': dfx})


def test_variables():
    expression = parser.parse('Ts_Skewness(open,4)+close / what')
    assert set(expression.variables()) == {'open', 'close', 'what'}
    
    
def test_product():
    expression = parser.parse('Product(open,2)')
    print expression.evaluate({'close': dfy, 'open': dfx})


def test_rank():
    expression = parser.parse('Rank(close)')
    print expression.evaluate({'close': dfy, 'open': dfx})


def test_tail():
    expression = parser.parse('Tail(close/open,0.99,1.01,1.0)')
    print expression.evaluate({'close': dfy, 'open': dfx})


def test_step():
    expression = parser.parse('Step(close,10)')
    print expression.evaluate({'close': dfy, 'open': dfx})


def test_decay_linear():
    expression = parser.parse('Decay_linear(open,2)')
    print expression.evaluate({'close': dfy, 'open': dfx})


def test_decay_exp():
    expression = parser.parse('Decay_exp(open, 0.5, 2)')
    print expression.evaluate({'close': dfy, 'open': dfx})


def test_sgined_power():
    expression = parser.parse('SignedPower(close-open, 2)')
    print dfx - dfy
    
    print expression.evaluate({'close': dfx, 'open': dfy})


@pytest.fixture(autouse=True)
def my_globals(request):
    ds = JzDataServer()
    
    df, msg = ds.daily("000001.SH, 600030.SH, 000300.SH", start_date=20170801, end_date=20170820,
                       fields="open,high,low,close,vwap,preclose")
    ds.api.close()
    
    multi_index_names = ['trade_date', 'security']
    df_multi = df.set_index(multi_index_names, drop=False)
    df_multi.sort_index(axis=0, level=multi_index_names, inplace=True)
    
    dfx = df_multi.loc[pd.IndexSlice[:, :], pd.IndexSlice['close']].unstack().T
    dfy = df_multi.loc[pd.IndexSlice[:, :], pd.IndexSlice['open']].unstack().T
    
    parser = Parser()
    request.function.func_globals.update({'parser': parser, 'dfx': dfx, 'dfy': dfy})


if __name__ == "__main__":
    pass