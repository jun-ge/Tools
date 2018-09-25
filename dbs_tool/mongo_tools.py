#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import time
import logging
from pymongo import MongoClient, errors


class MongoConnection(object):
    def __init__(self):
        self.logger = logging.getLogger('MongoConnection')

    def mongo_conn(self, **kwargs):
        while True:
            try:
                return MongoClient(kwargs.get('url'))[kwargs.get('db')]
            except errors.PyMongoError as e:
                self.logger.error("mongodb_conn链接失败,reconnect,error_msg:{}".format(e))
                time.sleep(3)
                continue


class MongoClientTools(object):
    def __init__(self, **kwargs):
        self.logger = logging.getLogger('MongoClientTools')
        self.__conn = MongoConnection().mongo_conn(**kwargs)

    def add(self, dict_data, table_name, find=None, many=False):
        try:
            if find:
                if getattr(self.__conn, table_name).find(find).count():
                    if many:
                        getattr(self.__conn, table_name).update_many(find, {'$set': dict_data})
                    else:
                        getattr(self.__conn, table_name).update(find, {'$set': dict_data})
                else:
                    self.logger.debug('匹配字段:{}无法查询到 %s' % find)
            else:
                getattr(self.__conn, table_name).insert(dict_data)
        except Exception as e:
            self.logger.error('data:{}, error_msg:{}'.format(str(dict_data), e))

    def search(self, table_name, find, sorted_key=None, reverse=False, limit=None):
        try:
            if isinstance(find, dict):
                if limit:
                    return getattr(self.__conn, table_name).find(find).sort({sorted_key: 1}).limit(limit) if not reverse \
                        else getattr(self.__conn, table_name).find(find).sort({sorted_key: -1}).limit(limit)
                else:
                    return getattr(self.__conn, table_name).find(find).sort(
                        {sorted_key: 1}) if not reverse else getattr(
                        self.__conn, table_name).find(find).sort({sorted_key: -1})
            elif isinstance(find, str):
                return getattr(self.__conn, table_name).distinct(find)
        except Exception as e:
            self.logger.error('search from key:{}, error_msg:{}'.format(str(find), e))

    def delete(self, table_name, find, many=True):
        """
        删除一条或多条数据
        :param table_name:
        :param find: 删除对象的key
        :param many: many 为True删除多个
        :return: A cursor / iterator over Mongo query results.
        """
        try:
            if many:
                getattr(self.__conn, table_name).delete_many(find)
            else:
                getattr(self.__conn, table_name).delete_one(find)
        except Exception as e:
            self.logger.error('search from key:{}, error_msg:{}'.format(str(find), e))


if __name__ == '__main__':
    conn = MongoConnection().mongo_conn()
    conn[''].find()