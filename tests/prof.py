import sys
from time import time
from datetime import date, datetime 
import sys, cProfile
from subprocess import Popen, PIPE

sys.path.append(sys.path[0].replace('\\', '/').rsplit('/', 1)[0] + '/pysqream')

import dbapi

cprof = cProfile.Profile() 
data = (False,2, 22, 222, 2222, 3.0, 4.0, "yada" , "yada" , date(2016, 12, 23), datetime(2016, 12, 23, 16, 56,45, 000)) 


def prof(amount=10**7):

   # con.start_profile()
   # con.CYTHON = use_cython

   create = 'create or replace table perf (b bool, t tinyint, sm smallint, i int, bi bigint, f real, d double, s varchar(10), ss nvarchar(10), dt date, dtt datetime)'
   con.execute(create)  

   # cprof.enable() 
   print ("Starting insert") 
   insert = 'insert into perf values (?,?,?,?,?,?,?,?,?,?,?)' 
   start = time() 
   con.executemany(insert, [data] * amount)  
   print (f"Total insert time for {amount} rows: {time() - start}")  
   # cprof.disable() 
   # cprof.dump_stats('koko.prof') 

   # Get data back if desired 
   con.execute('select count(*) from perf') 
   result = con.fetchall() 
   print (f"Count of inserted rows: {result[0][0]}") 

   # When done 
   con.close() 

   # con.dump_prof('dbapi_prof.json')
   # con.print_prof()


if __name__ == '__main__':

   args = sys.argv
   amount = 10**int(args[1]) if len(args)>1 else 10**6
   # use_cython = True if len(args)>2 and args[2] == 'cython' else False

   con = dbapi.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, False)
   prof(amount)
