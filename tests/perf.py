import os, sys,  cProfile
from time import time
from datetime import date, datetime 
from subprocess import Popen, PIPE
from math import log10


sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream/')
import dbapi

cprof = cProfile.Profile() 
perf_data = (False,2, 22, 222, 2222, 3.0, 4.0, "yada" , "yada" , date(2016, 12, 23), datetime(2016, 12, 23, 16, 56,45, 000)) 


col_type_data = {
   'bool'     : False,
   'tinyint'  : 2,
   'smallint' : 22,
   'int'      : 222,
   'bigint'   : 2222,
   'real'     : 3.0,
   'double'   : 4.0,
   'varchar'  : 'yada',
   'text'     : ('y'*10, 'y'*100, 'y'*1000),
   'text'     : 'y'*10,
   'date'     : date(2016, 12, 23),
   'datetime' : datetime(2016, 12, 23, 16, 56,45, 000)
}

col_type_to_prefix = {
   'bool'     : 'b',
   'tinyint'  : 't',
   'smallint' : 'sm',
   'int'      : 'i',
   'bigint'   : 'bi',
   'real'     : 'f',
   'double'   : 'd',
   'varchar' : 's',
   'text'     : 'ss',
   'date'     : 'dt',
   'datetime' : 'dtt'
}


def columns_ddl(col_types, amounts):

   # Single column passed
   if isinstance(col_types, str):
      res =  ', '.join([f'{col_type_to_prefix[col_types.split("(")[0]]}{i} {col_types}' for i in range(amounts)]) 
   else:
      res = ', '.join(f'{col_type_to_prefix[col_type.split("(")[0]]}{i} {col_type}' for (col_type, amount) in zip(col_types, amounts) for i in range(amount))

   return res


def prof_insert(col_setup = None, amount=10**6, table_name = 'perf'):

   # con.start_profile()
   # con.CYTHON = use_cython
   col_setup = col_setup or [('bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'varchar(10)', 'text', 'date', 'datetime'), (1,) * 11]
   create = f'create or replace table {table_name} ({columns_ddl(*col_setup)})'
   con.execute(create)  

   # cprof.enable() 
   print ("\nStarting insert") 
   # col_amount = len(create.split(','))
   col_amount = sum(col_setup[1])
   qmarks = '?, ' * (col_amount -1) + '?'
   insert = f'insert into {table_name} values ({qmarks})' 
   start = time() 
   data = sum([(col_type_data[col_type.split("(")[0]],) * amount for (col_type, amount) in zip(*col_setup)], ())
   con.executemany(insert, [data] * amount)  
   print (f"Total insert time for \033[94m10^{int(log10(amount))}\033[0m rows for table with setup \033[94m{list(zip(*col_setup))}\033[0m:\n\033[94m{time() - start}\033[0m")  
   # cprof.disable() 
   # cprof.dump_stats('koko.prof') 

   # Get data back if desired 
   con.execute(f'select count(*) from {table_name}') 
   result = con.fetchall() 
   print (f"Count of inserted rows: {result[0][0]}") 

   # When done 

   # con.dump_prof('dbapi_prof.json')
   # con.print_prof()


def prof_select(col_setup = None, amount=10**6, table_name = 'perf'):

   # con.start_profile()

   # Get data from table used in prof_insert()
   print (f"Retrieving {amount} rows") 
   # print (f"Retrieving {con.execute(f'select count(*) from {table_name}').fetchall()[0][0]} rows") 
   start = time()
   res = con.execute(f'select top {amount} * from {table_name}').fetchall() 
   print (f"Total select time: {time() - start}")  

   # When done 

   # con.dump_prof('dbapi_prof.json')
   # con.print_prof()


if __name__ == '__main__':

   args = sys.argv
   ip = args[1] if len(args) > 1 else '127.0.0.1'
   if len(args)>2:
      amount = 10**int(args[2])  
   # use_cython = True if len(args)>2 and args[2] == 'cython' else False

   con = dbapi.connect(ip, 5000, 'master', 'sqream', 'sqream', False, False)
   
   for col_type in col_type_data:
      for col_amount in (1, 10, 100):
         for row_amount in (1, 10**3, 10**4, 10**5, 10**6, 10**7):
            prof_insert([(col_type, ), (col_amount, )], row_amount)
   # prof_select(amount, table_name)

   con.close()
