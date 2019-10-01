import cherrypy
from jinja2 import Environment, FileSystemLoader
from query import EquityBhavCopy

env = Environment(loader=FileSystemLoader('templates'))


class StockAnalyzer(object):
    def __init__(self):
        self.parser = EquityBhavCopy()

    @cherrypy.expose
    def index(self):
        tmpl = env.get_template('index.html')
        return tmpl.render()

    @cherrypy.expose
    def top_stocks(self):
        tmpl = env.get_template('top_stocks.html')
        top_stocks, date = self.parser.get_top_stocks()
        context = {
            'stocks': top_stocks,
            'date': date
        }
        return tmpl.render(context)

    @cherrypy.expose
    def get_data(self):
        self.parser.get_latest_stocks()
        raise cherrypy.HTTPRedirect("/top_stocks")

    @cherrypy.expose
    def search(self, q=None):
        tmpl = env.get_template('search_result.html')
        if q:
            data = self.parser.get_stock_by_name(q)
        else:
            data = {}
        context = {
            'query': q,
            'stocks': data
        }
        return tmpl.render(context)


if __name__ == '__main__':
    cherrypy.quickstart(StockAnalyzer())
