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


def test_csv_diff():
    file_name = "driver_excel.xlsx"
    sheet_name = "file_to_file"
    print(len(read_excel(file_name, sheet_name)))  # len(read_excel())
    print(read_excel(file_name, sheet_name))
    for l in range(0, len(read_excel(file_name, sheet_name))):
        path = read_list(l, file_name, sheet_name)
        path0 = get_file_path("source_files" + os.sep + path[0])
        path1 = get_file_path("source_files" + os.sep + path[1])
        if path[6] == 'txt' or path[6] == 'csv' or path[6] == 'xls':
            print(path[2])
            cols = get_cols_uppercase(path[2], True)
        else:
            cols = get_cols_uppercase(path[2], False)
        if len(cols) > 1:
            path_1 = csv_generator_from_text_file(path0, path[6], cols)
            path_2 = csv_generator_from_text_file(path1, path[6], cols)
            path_3 = path[3]
            get_csv_differences(path_1, path_2, path_3)
        else:
            get_csv_differences(path0, path1, path[2])


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


def test_large_files():
    rand = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.option_context('display.colheader_justify', 'right')
    modified_file = "source_files" + os.sep + "sample2" + str(rand) + ".csv"
    lis = []
    # encoding='ISO-8859-1',na_filter=False
    file1 = get_file_path("source_files" + os.sep + "sample.csv")
    file2 = get_file_path("source_files" + os.sep + "oracle_gen_file_07_29_2022_11_51_24.csv")
    dh2 = pd.read_csv(file2, encoding='ISO-8859-1')
    print(dh2.shape)
    heds = dh2.columns.tolist()
    # ,"d5", "d6", "d7", "d8","d9", "d9", "d10", "d11"
    heds.extend(["d1", "d2", "d3", "d4"])
    print(len(heds))
    csv.register_dialect('skip_space', skipinitialspace=True)
    with open(file1, "r", encoding='ISO-8859-1', errors='strict') as csvfile:
        reader = csv.reader(csvfile, delimiter='|', quotechar='"', dialect='skip_space', skipinitialspace=True)
        for row in reader:
            lis.append(cell.strip() for cell in row)
    del lis[0]
    print(lis[0])
    print(heds)
    print(len(lis))
    N = 4
    dh1 = pd.DataFrame(lis, columns=heds)
    print(dh1.shape)
    dh1.drop(columns=dh1.columns[-N:], axis=1, inplace=True)
    print(dh1.shape)
    # di = dict(get_mismatched_dtypes(dh1, dh2))
    # print(di)
    dh1 = dh1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    dh1.to_csv(modified_file, index=False)


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


def test_file_to_db_diff():
    try:
        con_to = 'oracle'
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.option_context('display.colheader_justify', 'right')
        file_name = "DriverDBcomparison.xlsx"
        sheet_name = "FILE TO Oracle DB"
        print(len(read_excel(file_name, sheet_name)))
        print(read_excel(file_name, sheet_name))
        path = read_list(0, file_name, sheet_name)
        print(path)
        time_stamp_1 = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
        print(time_stamp_1)
        db_generated_file = "source_files" + os.sep + "oracle_gen_file_" + str(time_stamp_1) + ".csv"
        user = path[5]
        password = path[6]
        host = path[7]
        port = path[8]
        sid = path[9]
        dsn = host + str(port) + "/" + sid
        query1 = get_file_path("sql_query_files" + os.sep + path[10])
        f = open(query1, "r")
        full_sql = f.read()
        query1 = f'''{full_sql}'''
        print(query1)
        print(pyodbc.drivers())
        conn = cx_Oracle.connect(
            user=user, password=password,
            dsn=dsn)
        cur = conn.cursor()
        rows = cur.execute(query1).fetchall()
        cols = [col[0] for col in cur.description]
        df1 = pd.DataFrame.from_records(rows, columns=cols)
        print(df1.dtypes)
        df1 = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df1.to_csv(db_generated_file, index=False)
        cur.close()
        # ______________________ conversion _______________________________
        modified_file = "source_files" + os.sep + "sample_modified" + str(time_stamp_1) + ".csv"
        lis = []
        # encoding='ISO-8859-1',na_filter=False
        file1 = get_file_path("source_files" + os.sep + path[1])
        file2 = get_file_path(db_generated_file)
        dh2 = pd.read_csv(file2, encoding='ISO-8859-1')
        print(dh2.shape)
        heds = dh2.columns.tolist()
        heds.extend(["d1", "d2", "d3", "d4"])
        print(len(heds))
        No_of_header_records_to_remove = path[2]
        No_of_trailer_records_to_remove = path[3]
        csv.register_dialect('skip_space', skipinitialspace=True)
        with open(file1, "r", encoding='utf8', errors='strict') as csvfile:
            reader = csv.reader(csvfile, delimiter='|', quotechar='"', dialect='skip_space', skipinitialspace=True)
            for row in reader:
                lis.append(cell.strip() for cell in row)
        del lis[:No_of_header_records_to_remove]
        del lis[-No_of_trailer_records_to_remove:]
        del lis[0]
        print(lis[0])
        print(heds)
        print(len(lis))
        N = 4
        dh1 = pd.DataFrame(lis, columns=heds)
        print(dh1.shape)
        dh1.drop(columns=dh1.columns[-N:], axis=1, inplace=True)
        print(dh1.shape)
        di = dict(get_mismatched_dtypes(dh1, dh2))
        print(di)
        dh1 = dh1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        dh1.to_csv(modified_file, index=False)
        ls = path[11]
        cols = get_cols_uppercase(ls, True)
        file = get_file_path(modified_file)
        path_1 = csv_generator_from_text_file(file, "csv", cols)
        path_2 = csv_generator_from_text_file(file2, "csv", cols)
        path_3 = path[12]
        get_csv_differences(path_1, path_2, path_3)

    except cx_Oracle.IntegrityError as e:
        error = e.args
        print(error)



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
                                   database='DB_name',
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


def test_file_rows_deletion():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.option_context('display.colheader_justify', 'right')
    file_name = "DriverDBcomparison.xlsx"
    sheet_name = "FILE TO Oracle DB"
    print(len(read_excel(file_name, sheet_name)))
    print(read_excel(file_name, sheet_name))
    path = read_list(0, file_name, sheet_name)
    print(path)
    print(len(path))
    file1 = get_file_path("source_files" + os.sep + "sample2.csv")
    lis = []
    # file2 = get_file_path(db_generated_file)
    # dh2 = pd.read_csv(file2, encoding='ISO-8859-1')
    # print(dh2.shape)
    No_of_header_records_to_remove = path[2]
    No_of_trailer_records_to_remove = path[3]
    csv.register_dialect('skip_space', skipinitialspace=True)
    with open(file1, "r", encoding='ISO-8859-1', errors='strict') as csvfile:
        reader = csv.reader(csvfile, delimiter='|', quotechar='"', dialect='skip_space', skipinitialspace=True)
        for row in reader:
            lis.append(cell.strip() for cell in row)

    del lis[:No_of_header_records_to_remove]
    del lis[-No_of_trailer_records_to_remove:]
    heds = list(lis[0])
    heds.extend(["d1", "d2", "d3", "d4"])
    del lis[0]
    print(lis[0])
    print(heds)
    # ld = list(lis[0])
    # print(ld)
    # print(len(ld))
    print(type(lis[0]))
    # print(len(lis))
    # rm_cols_ls = get_cols_uppercase(path[4], False)
    # print(rm_cols_ls)
    N = 4
    dh1 = pd.DataFrame(lis)
    print(dh1.shape)
    # heds = dh1.columns.tolist()
    # heds.extend(["d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8"])
    # dh1 = dh1.drop(rm_cols_ls, axis=1)
    # dh1.drop(columns=dh1.columns[-N:], axis=1, inplace=True)
    dh1.to_csv("fiii.csv", index=False)
    print(dh1.shape)
    # csv.register_dialect('skip_space', skipinitialspace=True)
    # with open("fiii.csv", "r", encoding='utf8', errors='strict') as csvfile:
    #     reader = csv.reader(csvfile, delimiter='|', quotechar='"', dialect='skip_space', skipinitialspace=True)
    #     for row in reader:
    #         lis.append(cell.strip() for cell in row)
    # dh1 = pd.DataFrame(lis)
    # print(dh1.shape)
    # dh1.to_csv("fiii1.csv", index=False)
