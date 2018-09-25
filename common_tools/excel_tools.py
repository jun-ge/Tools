#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8


import xlrd
import xlwt


def excel_to_dict(filename, excel_sheet_num=0):
    """
    :rtype : object
    :return dict
    """
    # 打开文件
    data = xlrd.open_workbook(filename)
    # 获取表的几种方式
    # table = data.sheet_by_index()
    # table = data.sheet_by_name()
    # 获取第num个表
    table = data.sheets()[excel_sheet_num]
    # 获取行列
    rows = table.nrows
    cols = table.ncols
    dic = {}
    # 遍历获取每个格子的内容
    for i in range(1, rows):
        for j in range(0, cols):
            # 0行 j列获取标题
            title = table.cell_value(0, j)
            # i行j 列获取对应的内容
            value = table.cell_value(i, j)
            dic[title] = value
        # 使其成为生成器
        yield dic


def dict_to_excel(list_data, filename, sheetname):
    """

    :param list_data: [{},{},{}]类型
    :param filename:
    :param sheetname:
    :return:
    """
    file = xlwt.Workbook()
    table = file.add_sheet(sheetname, cell_overwrite_ok=True)
    titles = [u'code', u'cik', u'name', u'jys']
    # 初始化行列
    col = 0
    row = 0
    # 编写第一行
    for title in titles:
        table.write(0, col, title)
        col += 1
    # 编写内容
    for dict_data in list_data:
        col = 0
        for title in titles:
            try:
                table.write(row, col, str(dict_data[title]))
            except KeyError:
                continue
            col += 1
        row += 1
    file.save(filename)


if __name__ == '__main__':
    pass
