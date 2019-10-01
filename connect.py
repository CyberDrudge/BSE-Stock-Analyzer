import redis


def connect_redis():
    try:
        connect_db = redis.StrictRedis(host="localhost", charset="utf-8", decode_responses=True, port=6379, db=0)
        return connect_db
    except Exception:
        raise ConnectionError("Database Connection couldn't be Established")
