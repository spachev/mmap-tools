#! /usr/bin/env python

import MySQLdb
import argparse
from threading import Thread
import subprocess
import datetime
import sys
import re
import time
import glob
from MySQLdb.constants import CLIENT

def log_time(f):
    def wrap(*args):
        start = datetime.datetime.now()
        res = f(*args)
        t = (datetime.datetime.now() - start).total_seconds()
        info("Function {} ran in {} seconds".format(f.__name__, t))
        return res
    return wrap

def exec_cmd(cmd_args):
    return subprocess.check_output(cmd_args)

def find_csv(csv_base):
    return glob.glob(csv_base + '*.csv')

# for now we parse just the mysqldump format, not a generically formatted create table
# the assumption is that the dump contains only one table
def parse_create(sql_file):
    res = ""
    on_create = False
    with open(sql_file, "r") as fh:
        for l in fh:
            if l.startswith("CREATE TABLE"):
                on_create = True
            if on_create:
                res += l
            if l.startswith(");"):
                return res

def read_file(fname):
    with open(fname, 'r') as fp:
        return ''.join([l for l in fp])

def info(msg):
    print("[" + str(datetime.datetime.now()) + "] " + str(msg))

def debug(msg):
    if args.debug:
        print("[" + str(datetime.datetime.now()) + "] " + str(msg))

def error(msg):
    print("[" + str(datetime.datetime.now()) + "] " + str(msg))

def get_write_rows(cursor):
    cursor.execute("select variable_value from information_schema.global_status where variable_name = 'Handler_write'")
    res = cursor.fetchall()
    return int(res[0][0])


# we assume little to none other activity on the server
def monitor_progress(start_write_rows):
    global done
    cursor = get_db_cursor()
    last_write_rows = start_write_rows
    last_ts =  datetime.datetime.now()
    start_ts = last_ts
    while not done:
        cur_write_rows = get_write_rows(cursor)
        cur_ts = datetime.datetime.now()
        cur_dt = (cur_ts - last_ts).total_seconds()
        cur_drows = cur_write_rows - last_write_rows
        start_dt_native = cur_ts - start_ts
        start_dt = start_dt_native.total_seconds()
        start_drows = cur_write_rows - start_write_rows
        cur_rate = int(cur_drows / cur_dt)
        avg_rate = int(start_drows / start_dt)
        info("{} since start, total rows {}, avg rate {} rows/s, cur rate {} rows/s".
            format(start_dt_native, start_drows, avg_rate, cur_rate))
        last_write_rows  = cur_write_rows
        last_ts = cur_ts
        time.sleep(5) # todo - configurable sleep interval


def get_db_cursor():
    global con_args
    con = MySQLdb.connect(
        **con_args
    )
    cursor = con.cursor()
    return cursor

def prepare_table(sql_file, extra_sql_file, drop_if_exists):
    create_table = parse_create(sql_file)
    table_name = parse_table_from_create(create_table)
    cursor = get_db_cursor()

    if drop_if_exists:
        # do not worry about SQL-injection as we are reading dump files
        # if we cannot trust them, we should not be loading them in the first place
        cursor.execute("drop table if exists `" + table_name + "`")
    cursor.execute(create_table)
    if extra_sql_file:
        cursor.execute(read_file(extra_sql_file))
    return table_name

def load_file_worker(table_name, file_name):
    cursor = get_db_cursor()
    table_name = table_name.replace("`", "") # healthy paranoia
    query = ("load data local infile %s into table `" +
                table_name + "` fields terminated by ',' optionally enclosed by \"'\"")
    cursor.execute(query, (file_name,))
    cursor.execute("commit")

# for now just assume CREATE TABLE `tbl` with no backs in the name of the table
# TODO: make more robust
def parse_table_from_create(create_table):
    return create_table.split("`")[1]

def create_csv_files(sql_file, csv_base, num_csv_files):
    cmd_args = [args.mysqldump2csv, "--input-file=" + sql_file,
                "--n-out-files=" + str(num_csv_files), "--output-base=" + csv_base]
    exec_cmd(cmd_args)

parser = argparse.ArgumentParser()
parser.add_argument("--port", help="MySQL port", type=int, default=3306)
parser.add_argument('--host', help="MySQL host", default="127.0.0.1")
parser.add_argument('--user', help="MySQL user")
parser.add_argument('--db', help="MySQL database", required=True)
parser.add_argument('--read-default-file', help='MySQL defaults file', default='~/.my.cnf')

parser.add_argument('--csv-base', help="The name of the csv files without N.csv suffix", required=True)
parser.add_argument('--sql-file', help="File produced by mysqldump", required=True)
parser.add_argument('--extra-sql-file', help="run commands from this file after creating the table")
parser.add_argument('--mysqldump2csv', help="location of the mysqldump2csv binary", default="mysqldump2csv")
parser.add_argument('--num-csv-files', help='number of CSV files to split the dump into', default="4")
parser.add_argument('--drop-table-if-exists', help='drop the table if it exists', action="store_true")

args = parser.parse_args()
dict_args = vars(args)
con_args = {"client_flag" : CLIENT.LOCAL_FILES}
for k in ["port", "host", "user", "db", "read_default_file"]:
    con_args[k] = dict_args[k]

csv_files = find_csv(args.csv_base)
table_name = prepare_table(args.sql_file, args.extra_sql_file, args.drop_table_if_exists)
if not csv_files:
    create_csv_files(args.sql_file, args.csv_base, args.num_csv_files)
    # here we assume nobody creates additional csv files under the rug
    csv_files = find_csv(args.csv_base)

threads = []
done = False
cursor = get_db_cursor()
monitor_th = Thread(target=monitor_progress, args=(get_write_rows(cursor),))
monitor_th.start()
for f in csv_files:
    th = Thread(target=load_file_worker, args=(table_name, f))
    threads.append(th)
    th.start()

for th in threads:
    th.join()

done = True

monitor_th.join()
