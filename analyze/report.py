# encoding: utf-8

from jinja2 import Environment, FileSystemLoader
import pandas as pd
from weasyprint import HTML


class Report(object):
    def __init__(self, d, template_fp, css_fp, out_folder='output'):
        """Please use absolute paths."""
        self.dic = d
        self.fp_temp = template_fp
        self.fp_css = css_fp
        self.out_folder = out_folder
        self.html = ""
        
        self.env = Environment(loader=FileSystemLoader(['', '/', '.', '..']))
        self.update_env()
        self.template = self.env.get_template(self.fp_temp)
    
    def update_env(self):
        def round_if_float(x, n):
            if isinstance(x, float):
                return round(x, n)
            else:
                return x
        self.env.filters.update({'round_if_float': round_if_float})
    
    def generate_html(self):
        self.html = self.template.render(self.dic)
    
    def output_html(self, fn='test_out.html'):
        with open(self.out_folder + '/' + fn, 'w') as f:
            f.write(self.html)
    
    def output_pdf(self, fn='test_out.html'):
        h = HTML(string=self.html)
        h.write_pdf(self.out_folder + '/' + fn)#, stylesheets=[self.fp_css])


def test_output():
    r = Report({'mytitle': 'Test Title', 'mytable': 'Hello World!'},
               'static/test_template.html', 'static/blueprint.css')
    r.generate_html()
    print r.html
    r.output_html()
    r.output_pdf()


def main():
    pass


if __name__ == "__main__":
    # main()
    test_output()
