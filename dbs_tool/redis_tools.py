#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import redis
import logging
import time


class RedisConnection(object):
    def __init__(self):
        self.logger = logging.getLogger('RedisConnect')
        self.retry = 0

    def redis_connect(self, **kwarge):
        try:
            redis_conn = redis.Redis(host=kwarge.get('host', 'localhost'),
                                     port=kwarge.get('port', 6379),
                                     password=kwarge.get('password'),
                                     db=kwarge.get('db', 0)
                                     )
            return redis_conn
        except Exception as e:
            self.logger.error('redis连接出错，error_msg: %s' % e)
            time.sleep(1)
            self.retry += 1
            self.logger.error('正在进行第%s次重连' % self.retry)
            if self.retry < 3:
                self.redis_connect(**kwarge)
            return None


class RedisClient(object):

    def __init__(self, **kwargs):
        self.logger = logging.getLogger('RedisTools')
        self.__conn = RedisConnection().redis_connect(**kwargs)

    """
    根据实际项目需求添加不同功能
    
    """
    def get(self):
        pass

    def put(self):
        pass

    def pop(self):
        pass






if __name__ == '__main__':
    # 能直接处理的操作
    conn = RedisConnection().redis_connect()
    # 包含多个redis操作的方法
    redis_tool = RedisClient()


