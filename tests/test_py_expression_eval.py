# encoding: UTF-8

import pandas as pd
import pytest
from quantos.data.dataserver import JzDataServer

from quantos.data.py_expression_eval import Parser


def test_skew():
    parser.set_capital('lower')
    expression = parser.parse('ts_skewness(open,4)')
    print parser.evaluate({'close': dfy, 'open': dfx})


def test_variables():
    expression = parser.parse('Ts_Skewness(open,4)+close / what')
    assert set(expression.variables()) == {'open', 'close', 'what'}
    
    
def test_product():
    parser.set_capital('lower')
    expression = parser.parse('product(open,2)')
    print parser.evaluate({'close': dfy, 'open': dfx})


def test_rank():
    expression = parser.parse('Rank(close)')
    print parser.evaluate({'close': dfy, 'open': dfx})


def test_tail():
    expression = parser.parse('Tail(close/open,0.99,1.01,1.0)')
    print parser.evaluate({'close': dfy, 'open': dfx})


def test_step():
    expression = parser.parse('Step(close,10)')
    print parser.evaluate({'close': dfy, 'open': dfx})


def test_decay_linear():
    expression = parser.parse('Decay_linear(open,2)')
    print parser.evaluate({'close': dfy, 'open': dfx})


def test_decay_exp():
    expression = parser.parse('Decay_exp(open, 0.5, 2)')
    print parser.evaluate({'close': dfy, 'open': dfx})


def test_sgined_power():
    expression = parser.parse('SignedPower(close-open, 2)')
    print dfx - dfy
    
    print parser.evaluate({'close': dfx, 'open': dfy})


def test_group_apply():
    import numpy as np
    np.random.seed(369)
    
    n = 20
    
    dic = {c: np.random.rand(n) for c in 'abcdefghijklmnopqrstuvwxyz'[:n]}
    df_value = pd.DataFrame(index=range(n), data=dic)
    
    r = np.random.randint(0, 5, n * df_value.shape[0]).reshape(df_value.shape[0], n)
    cols = df_value.columns.values.copy()
    np.random.shuffle(cols)
    
    df_group = pd.DataFrame(index=df_value.index, columns=cols, data=r)
    
    expr = parser.parse('GroupApply(Standardize, close, mygroup)')
    res = parser.evaluate({'close': df_value, 'mygroup': df_group})
    
    assert abs(res.iloc[3, 6] - (-1.53432)) < 1e-5
    assert abs(res.iloc[19, 18] - (-1.17779)) < 1e-5


@pytest.fixture(autouse=True)
def my_globals(request):
    ds = JzDataServer()
    
    df, msg = ds.daily("000001.SH, 600030.SH, 000300.SH", start_date=20170801, end_date=20170820,
                       fields="open,high,low,close,vwap,preclose")
    ds.api.close()
    
    multi_index_names = ['trade_date', 'symbol']
    df_multi = df.set_index(multi_index_names, drop=False)
    df_multi.sort_index(axis=0, level=multi_index_names, inplace=True)
    
    dfx = df_multi.loc[pd.IndexSlice[:, :], pd.IndexSlice['close']].unstack().T
    dfy = df_multi.loc[pd.IndexSlice[:, :], pd.IndexSlice['open']].unstack().T
    
    parser = Parser()
    request.function.func_globals.update({'parser': parser, 'dfx': dfx, 'dfy': dfy})


if __name__ == "__main__":
    pass
