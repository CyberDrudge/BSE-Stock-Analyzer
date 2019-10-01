from connect import connect_redis
from fetch import BhavCopyFetch


class EquityBhavCopy:
    def __init__(self):
        self.extractor = BhavCopyFetch()
        self.page_size = 10

    def get_top_stocks(self, page_no=0):
        redis_conn = connect_redis()
        start_index = page_no * self.page_size
        end_index = start_index + self.page_size - 1
        keys = redis_conn.zrevrange("search_sorted", start_index, end_index, withscores=False)
        top_stocks = []
        for key in keys:
            reg = redis_conn.hgetall(key)
            top_stocks.append(reg)
        latest_date = redis_conn.get("latest_date")
        return top_stocks, latest_date

    def get_stock_by_name(self, name):
        redis_conn = connect_redis()
        keys = redis_conn.scan_iter(match='*' + str(name).upper() + '*')
        query_result = []
        for key in keys:
            reg = redis_conn.hgetall(key)
            query_result.append(reg)
        return query_result

    def get_latest_stocks(self):
        self.extractor.load_zip_to_redis()
