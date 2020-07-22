import os, sys, io
from time import time
from datetime import date, datetime 
from subprocess import Popen, PIPE
from math import log10


sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream/')
import dbapi


col_type_data = {
   'bool'          : False,
   'tinyint'       : 2,
   'smallint'      : 22,
   'int'           : 222,
   'bigint'        : 2222,
   'real'          : 3.0,
   'double'        : 4.0,
   'varchar(10)'   : 'y'*10,
   'varchar(100)'  : 'y'*100,
   'varchar(1000)' : 'y'*1000,
   'text*10'       : 'y'*10,
   'text*100'      : 'y'*100,
   'text*1000'     : 'y'*1000,
   'text'       : 'y'*10,
   'date'          : date(2016, 12, 23),
   'datetime'      : datetime(2016, 12, 23, 16, 56,45, 000)
}

col_type_to_prefix = {
   'bool'          : 'b',
   'tinyint'       : 't',
   'smallint'      : 'sm',
   'int'           : 'i',
   'bigint'        : 'bi',
   'real'          : 'f',
   'double'        : 'd',
   'varchar'       : 's',
   'varchar(10)'   : 's_10_',
   'varchar(100)'  : 's_100_',
   'varchar(1000)' : 's_1000_',
   'text'          : 'ss',
   'text*10'       : 'ss_10_',
   'text*100'      : 'ss_100_',
   'text*1000'     : 'ss_1000_',
   'date'          : 'dt',
   'datetime'      : 'dtt'
}


def columns_ddl(col_types, amounts):
   ''' Generate the better part of a create table query based on column types and amounts '''

   # Single column passed
   if isinstance(col_types, str):
      res =  ', '.join([f'{col_type_to_prefix[col_types]}{i} {col_types.split("*")[0]}' for i in range(amounts)]) 
   else:
      res = ', '.join(f'{col_type_to_prefix[col_type]}{i} {col_type.split("*")[0]}' for (col_type, amount) in zip(col_types, amounts) for i in range(amount))

   return res


def prof_insert(col_setup = None, amount=10**6, table_name = 'perf'):
   ''' Profile insert for a specific column setup and row amount '''

   global con

   col_setup = col_setup or [('bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'varchar(10)', 'text', 'date', 'datetime'), (1,) * 11]
   create = f'create or replace table {table_name} ({columns_ddl(*col_setup)})'
   con.execute(create)  

   print (f"\nTable setup: \033[94m{list(zip(*col_setup))}\033[0m, Rows: \033[94m10^{int(log10(amount))}\033[0m")
   # col_amount = len(create.split(','))
   col_amount = sum(col_setup[1])
   qmarks = '?, ' * (col_amount -1) + '?'
   insert = f'insert into {table_name} values ({qmarks})' 
   start = time() 
   data = sum([(col_type_data[col_type],) * amount for (col_type, amount) in zip(*col_setup)], ())
   con.executemany(insert, [data] * amount)  
   print (f"Total insert time: \033[94m{time() - start}\033[0m")  

   # Count data
   print (f"Count of inserted rows: {con.execute(f'select count(*) from {table_name}').fetchall()[0][0]}") 



def prof_select(col_setup = None, amount=10**6, table_name = 'perf'):

   global con

   # Get data from table used in prof_insert()
   print (f"Retrieving {amount} rows") 
   # print (f"Retrieving {con.execute(f'select count(*) from {table_name}').fetchall()[0][0]} rows") 
   start = time()
   res = con.execute(f'select top {amount} * from {table_name}').fetchall() 
   print (f"Total select time: {time() - start}")  


## --- Test setups ---
#
row_amounts = 1, 10**3, 10**4, 10**5, 10**6, 10**7

def single_type_test():
   ''' Checking performance for single type tables '''
   
   col_type_data = {
      'varchar(1000)' : 'y'*1000,
      'text*1000'     : 'y'*1000,
      'date'          : date(2016, 12, 23),
      'datetime'      : datetime(2016, 12, 23, 16, 56,45, 000)
   }

   # Single type test
   for col_type in col_type_data:
      for col_amount in (1, 10, 100):
         single_row_amounts = row_amounts[:3]
         for row_amount in single_row_amounts:
            prof_insert([(col_type, ), (col_amount, )], row_amount)
            # prof_select(amount, table_name)


def mixed_types_test():
   ''' 18 of each column type '''

   col_type_data = {
      'bool'          : False,
      'tinyint'       : 2,
      'smallint'      : 22,
      'int'           : 222,
      'bigint'        : 2222,
      'real'          : 3.0,
      'double'        : 4.0,
      'varchar(10)'   : 'y'*10,
      'text'          : 'y'*10,
      'date'          : date(2016, 12, 23),
      'datetime'      : datetime(2016, 12, 23, 16, 56,45, 000)
   }

   for row_amount in (10**6,): #row_amounts:
      prof_insert([col_type_data.keys(), (18,) * len(col_type_data)], row_amount)
      # prof_select(amount, table_name)
      

if __name__ == '__main__':

   # global con
   args = sys.argv
   ip = args[1] if len(args) > 1 else '127.0.0.1'
   con = dbapi.connect(ip, 5000, 'master', 'sqream', 'sqream', False, False)
   # single_type_test()
   mixed_types_test()
   
   con.close()
