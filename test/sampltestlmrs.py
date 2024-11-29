import csv
import datetime
import os
import re
from glob import glob

import cx_Oracle
import pandas as pd
import dask.dataframe as dd
import pyodbc
from cx_Oracle import DatabaseError

from test.utils import read_list, get_csv_differences, get_cols_uppercase, \
    csv_generator_from_text_file, read_excel, get_file_path, get_mismatched_dtypes, get_lmrs_csv_differences


def test_cleanup_folders():
    # print(os.getcwd())
    pth = r"{}{val}test{val}subFiles{val}*".format(os.getcwd(), val=os.sep)
    pth1 = r"{}{val}test{val}converted_files{val}*".format(os.getcwd(), val=os.sep)
    pth2 = r"{}{val}test{val}Final_Csv_Files{val}*".format(os.getcwd(), val=os.sep)
    pth3 = r"{}{val}test{val}Final_Html_Files{val}*".format(os.getcwd(), val=os.sep)
    li = [pth, pth1, pth2, pth3]
    print(li)
    for i in li:
        files = glob(i)
        for items in files:
            os.remove(items)
    print("Successfully Cleaned up all the folders")




def replaces(li):
    new_list = []
    for i in li.split("|"):
        if re.match(r'"([^"])"', i):
            l = str(i).replace('|', '_').replace(" ", "")
            new_list.append(l)
        else:
            new_list.append(i)
    print(new_list)


# pytest -v test\sampletest.py::test_csv_diff
# pytest -v test\sampletest.py::test_cleanup_folders
# pytest -v test\sampletest.py::test_db_connect
# pytest -v test\sampletest.py::test_large_files




def test_format():
    query1 = "C:\\Users\\vniss02\\PycharmProjects\\SDR_Automation\\test\\sdr_query1.sql"
    f = open(query1, "r")
    full_sql = f.read()
    query1 = f'''{full_sql}'''
    print(query1)


# sst: sql server to sql server comparision
def test_ss_db_to_ss_db_comparison():
    try:
        print(pyodbc.drivers())

        time_stamp_1 = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.option_context('display.colheader_justify', 'right')
        file_name = "DriverDBcomparison.xlsx"
        sheet_name = "LMRS DB to LMRS DB"
        print(len(read_excel(file_name, sheet_name)))
        print(read_excel(file_name, sheet_name))
        for l in range(0, len(read_excel(file_name, sheet_name))):
            path = read_list(l, file_name, sheet_name)
            print(path)
            print(len(path))
            modified_file_sit = "source_files" + os.sep + "sqlServer_gen_file_sit_" + path[5] + str(
                time_stamp_1) + ".csv"
            modified_file_uat = "source_files" + os.sep + "sqlServer_gen_file_uat_" + path[5] + str(
                time_stamp_1) + ".csv"
            query1 = get_file_path("sql_query_files" + os.sep + path[5])
            f = open(query1, "r")
            full_sql = f.read()
            query1 = f'''{full_sql}'''
            print(query1)
            host_sit = path[1]
            host_uat = path[6]
            port = path[4]

            conn = pyodbc.connect(DRIVER='{ODBC Driver 17 for SQL Server}',
                                  Server=f'{host_sit},{port}',
                                  database='db_name',
                                  Trusted_connection='yes',
                                  # username="IBG\\vniss02",
                                  # password='',
                                  autocommit=True)
            dh1 = pd.read_sql_query(sql=query1, con=conn)
            dh1['BUSINESS_DT'] = pd.to_datetime(dh1['BUSINESS_DT'], format='%d/%m/%y %H:%M:%S').dt.strftime(
                '%Y-%m-%d %H:%M:%S')
            dh1.to_csv(modified_file_sit, index=False)
            print(dh1.shape)

            conn2 = pyodbc.connect(DRIVER='{ODBC Driver 17 for SQL Server}',
                                   Server=f'{host_uat},{port}',
                                   database='dbname',
                                   Trusted_connection='yes',
                                   # username="IBG\\vniss02",
                                   # password='',
                                   autocommit=True)
            dh2 = pd.read_sql_query(sql=query1, con=conn2)
            dh2['BUSINESS_DT'] = pd.to_datetime(dh2['BUSINESS_DT'], format='%d/%m/%y %H:%M:%S').dt.strftime(
                '%Y-%m-%d %H:%M:%S')
            dh2.to_csv(modified_file_uat, index=False)
            print(dh2.shape)
            assert dh1.shape == dh2.shape
            ls = path[10]
            cols = get_cols_uppercase(ls, True)
            # file = get_file_path(modified_file)
            path_1 = csv_generator_from_text_file(modified_file_sit, "csv", cols)
            path_2 = csv_generator_from_text_file(modified_file_uat, "csv", cols)
            path_3 = path[11]
            path_4 = path[12]
            get_lmrs_csv_differences(path_1, path_2, path_3, path_4)

    except Exception as e:
        error = e.args
        print(error)

