# encoding: utf-8
from quantos.backtest.analyze.report import Report
from quantos import SOURCE_ROOT_DIR
import os


def test_output():
    static_folder = os.path.join(SOURCE_ROOT_DIR, 'backtest/analyze/static')
    
    r = Report({'mytitle': 'Test Title', 'mytable': 'Hello World!'},
               os.path.join(static_folder, 'test_template.html'),
               os.path.join(static_folder, 'blueprint.css'),
               out_folder='../output')
    r.generate_html()
    r.output_html()
    r.output_pdf()


if __name__ == "__main__":
    test_output()
