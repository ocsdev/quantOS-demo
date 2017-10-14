# encoding: utf-8
from quantos.backtest.analyze.report import Report
from quantos.util import fileio


def test_output():
    static_folder = fileio.join_relative_path('backtest/analyze/static')

    r = Report({'mytitle': 'Test Title', 'mytable': 'Hello World!'},
               source_dir=static_folder,
               template_fn='test_template.html',
               css_fn='blueprint.css',
               out_folder='../output')
    r.generate_html()
    r.output_html()
    r.output_pdf()


if __name__ == "__main__":
    test_output()
