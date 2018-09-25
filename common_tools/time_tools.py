#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8
import time
import pytz
import logging
from datetime import datetime

tz_us = pytz.timezone('America/New_York')
tz_cn = pytz.timezone('Asia/Shanghai')


def format_str_to_time(time_str, format_str, *args):
    try:
        _date = datetime.strptime(time_str, format_str)
        return _date
    except Exception as e:
        logging.error(e)
        for format_arg in args:
            if format_arg is None:
                continue
            try:
                _date = datetime.strptime(time_str, format_arg)
                return _date
            except Exception as e1:
                logging.error(e1)


def format_time_to_str(date, format_str):
    return date.strftime(format_str)


def get_format_timezone(msec=False):
    return str(time.time()).replace('.', '') if msec else str(time.time())


if __name__ == '__main__':
    print format_time_to_str(format_str_to_time('1990/08/08', '%Y-%m-%d', '%Y~%m~%d', '%Y/%m/%d', '%Y:%m:%d'),
                             '%Y~%m~%d')
    # print get_format_timezone(msec=True)
