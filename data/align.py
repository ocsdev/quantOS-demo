# encoding: utf-8
import numpy as np
import pandas as pd
from data.py_expression_eval import Parser
from data.dataserver import JzDataServer


def get_neareast(df_ann, df_value, date):
    date = date[0]
    res = np.where(date - df_ann.values >= 0, df_value, np.nan)
    df = pd.DataFrame(res).fillna(method='ffill', axis=0)
    res = df.as_matrix()
    
    return res[-1, :]
    

def align(df_ann, df_value, date_arr):
    date_arr = np.asarray(date_arr, dtype=int)
    
    df_formula = df_value
    
    res = np.apply_along_axis(lambda date: get_neareast(df_ann, df_formula, date), 1, date_arr.reshape(-1, 1))

    df_res = pd.DataFrame(index=date_arr, columns=df_value.columns, data=res)
    return df_res
    
    
def main():
    ann_date1 = [20170301, 20170609, 20170902]
    ann_date2 = [20170302, 20170607, 20170930]
    value1 = [3, 5, 7]
    value2 = [2, 4, 6]
    
    df_sec_quarter = pd.DataFrame(data={'value1': value1, 'ann_date1': ann_date1, 'value2': value2, 'ann_date2': ann_date2})
    
    print df_sec_quarter
    
    date_arr = range(20170601, 20170612, 1)
    align(df_sec_quarter, date_arr)


def main_large():
    fp = 'align.csv'
    raw = pd.read_csv(fp)
    raw.columns = [u'security', u'ann_date', u'report_period', u'oper_rev', u'oper_cost']
    raw.drop(['oper_cost'], axis=1, inplace=True)
    print raw.columns
    print raw.index
    
    idx_list = ['report_period', 'security']
    raw_idx = raw.set_index(idx_list)
    raw_idx.sort_index(axis=0, level=idx_list, inplace=True)
    
    # stk = raw_idx.stack()
    # ustk = stk.unstack(level=1)
    ###
    df_ann = raw_idx.loc[pd.IndexSlice[:, :], 'ann_date']
    df_ann = df_ann.unstack(level=1)

    df_value = raw_idx.loc[pd.IndexSlice[:, :], 'oper_rev']
    df_value = df_value.unstack(level=1)
    ###
    
    """
    df_ann = ustk.loc[pd.IndexSlice[:, 'ann_date'], pd.IndexSlice[:]]
    df_ann.fillna(-1, inplace=True)
    mat = df_ann.as_matrix().astype(int)
    df_ann = pd.DataFrame(index=df_ann.index, columns=df_ann.columns, data=mat)
    df_ann.replace(-1, np.nan, inplace=True)
    df_ann.index = df_ann.index.droplevel(level=1)
    
    df_value = ustk.loc[pd.IndexSlice[:, 'oper_rev'], pd.IndexSlice[:]]
    df_value.index = df_value.index.droplevel(level=1)
    
    """
    parser = Parser()
    expression = parser.parse('Delta(revenue, 1) / Delay(revenue,1) + income')
    
    exp = 'MyInd * close'
    
    exp2 = 'Delta(rev_q, 2)'
    
    df_exp2 = exp2.evaluate()
    
    df_exp2_align = align(df_exp2. begin, end)
    
    exp1.evaluate('MyInd' : df_exp2)
    
    df_evaluate = expression.evaluate({'revenue': df_value, 'income': df_income})
    
    # df_sec_quarter = pd.DataFrame(data={'value1': value1, 'ann_date1': ann_date1, 'value2': value2, 'ann_date2': ann_date2})
    
    # print df_sec_quarter
    
    ds = JzDataServer()
    date_arr = ds.get_trade_date(20160325, 20170625)
    df_res = align(df_ann, df_evaluate, date_arr)
    sec = '600000.SH'
    print "\nValue:"
    print df_value.loc[:, sec]
    print "\nEvaluation"
    print df_evaluate.loc[:, sec]
    print "\nANN_DATE"
    print df_ann.loc[:, sec]
    print "TEST"
    print "20161028  {:.4f}".format(df_res.loc[20161028, sec])
    print "20161031  {:.4f}".format(df_res.loc[20161031, sec])
    print "20170427  {:.4f}".format(df_res.loc[20170427, sec])
    
    print

if __name__ == "__main__":
    import time
    t_start = time.time()
    
    main_large()
    
    t3 = time.time() - t_start
    print "\n\n\nTime lapsed in total: {:.1f}".format(t3)
