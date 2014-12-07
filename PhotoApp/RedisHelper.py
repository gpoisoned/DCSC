import redis

class RedisHelper:
    def __init__(self, host, db):
        self.host_name = host
        self.db = db
        self.setup()

    def setup(self):
        self.redis = redis.Redis(host=self.host, db=self.db)

    """
        sendList: stores the (list) data to specified key in redis
        If the 'key' already exists, it will append data to that list
        If the 'key' doesn't exist, it will create the list and append data

        If the 'data' already exists in the list, this won't add a duplicate
    """
    def sendList(self, key, data):
        if (data is not None) and (type(data) is list):
            current_redis_data = None
            if self.keyExists(key):
                current_redis_data = self.redis.lrange(key, 0, -1)

            for value in data:
                if current_redis_data:
                    if value not in current_redis_data:
                        self.redis.lpush(key, value)
                else:
                    # If it gets here, the key exists but data is empty
                    # So, simply add the new value
                    self.redis.lpush(key, value)
            return True
        else
            return False

    """
        getList: retuns all the values in the redis list
        If the 'key' already exists, it returns all the values in the list
        If the 'key' doesn't exist, it will return empty list []
    """
    def getList(self, key, start= 0, end =-1):
        if self.keyExists(key):
            return self.redis.lrange(key, start, end)

    def keyExists(self, key):
        return self.redis.exists(key)
