#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8


import time
import pymysql
import logging


class MysqlConnection(object):
    def __init__(self):
        self.logger = logging.getLogger('MySQLConnect')

    def mysql_conn(self, **kwargs):
        while True:
            try:
                if not kwargs.get('charset'):
                    kwargs['charset'] = 'utf8mb4'
                connection = pymysql.connect(host=kwargs.get('host', 'localhost'),
                                             user=kwargs.get('user', 'root'),
                                             port=kwargs.get('port', '3306'),
                                             password=kwargs.get('password', 'root'),
                                             db=kwargs.get('db'),
                                             charset=kwargs.get('charset'),
                                             cursorclass=pymysql.cursors.DictCursor)
                return connection
            except pymysql.MySQLError as e:
                self.logger.error("链接mysql出错,kwargs:{},error_msg:{}".format(kwargs, e))
                time.sleep(3)
                continue


class MysqlClient(object):
    def __init__(self, **kwargs):
        self.logger = logging.getLogger('MysqlClient')
        self.__conn = MysqlConnection().mysql_conn(**kwargs)

    def selete_field(self, field, table_name, where='', limit=None):
        try:
            if limit:
                with self.__conn.cursor() as cursor:
                    sql = "select %s from %s %s limit %d" % (','.join(field), table_name, where, limit)
                    self.logger.debug('select_field_limit:{}'.format(sql))
                    cursor.execute(sql)
                    return cursor.fetchall()
            else:
                with self.__conn.cursor() as cursor:
                    sql = "select %s from %s %s" % (','.join(field), table_name, where)
                    self.logger.debug('select_field_limit:{}'.format(sql))
                    cursor.execute(sql)
                    return cursor.fetchall()
        except Exception as e:
            sql = "select %s from %s %s limit %d" % (','.join(field), table_name, where, limit)
            self.logger.error('sql:{}, error_msg:{}'.format(sql, e))
            return False

    def exec_sql(self, sql, args=None):
        try:
            self.logger.debug("sql:{},args:{}".format(sql, args))
            with self.__conn.cursor() as cursor:
                cursor.execute(sql, args)
                self.__conn.commit()
                return True
        except Exception as e:
            self.logger.error('sql:%s,args:%s,error_msg:%s' % (sql, args, e))
            return False

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y/%m/%d %H:%M:%S',
                        filename='1.log',
                        filemode='w')
    mysql_conn = MysqlConnection().mysql_conn()
    mysql_tool = MysqlClient()


