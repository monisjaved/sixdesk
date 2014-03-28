import sqlite3,time
import psutil, os, MySQLdb

#set high priority
p = psutil.Process(os.getpid())
p.set_nice(psutil.HIGH_PRIORITY_CLASS)

#time initialisation
start = time.time()

#opening connection
try:
	conn = MySQLdb.connect("localhost","root","","python")
	conn.autocommit(False)
	cur = conn.cursor()
except sqlite3.Error:
	print "error"
	exit(1)	
print "Opened database successfully";

#creating table
sql = "create table if not exists input("
for i in range(1,7):
	sql += "i"+str(i)+" double,"
sql = sql[:-1] + ")"
cur.execute(sql)
sql = "SHOW TABLES like 'input'"
cur.execute(sql)
for row in cur:
	print row

	
#variable
value = 1
val = 1
tablename = "input"
#PRAGMA settings
# cur.execute("PRAGMA synchronous = OFF")
# cur.execute("PRAGMA journal_mode = MEMORY")
# cur.execute("PRAGMA auto_vacuum = FULL")
# cur.execute("PRAGMA temp_store = MEMORY")
# cur.execute("PRAGMA count_changes = OFF")
# cur.execute("PRAGMA mmap_size=2335345345") 
conn.query('SET autocommit=0;')
conn.query('SET unique_checks=0; ')
conn.query('SET foreign_key_checks=0;')
conn.query('LOCK TABLES %s WRITE;' % (tablename))
conn.query('START TRANSACTION;')
conn.query('ALTER TABLE %s DISABLE KEYS;' % (tablename))

#loop for insertion of data
for i in range(0,10):
	#cur.execute("begin immediate transaction")
	matrix = []
	for j in range(0,10000):
		#cur.execute('INSERT into input (i1,i2,i3,i4,i5,i6) values(?,?,?,?,?,?)',(value,++value,++value,++value,++value,++value))	
		matrix.append((value,value+1,value+2,value+3,value+4,value+5))
		value += 6
	cur.executemany('INSERT into input values(%s,%s,%s,%s,%s,%s)',matrix)
	conn.commit()
	
#commit	
#conn.commit()

#time conclude
conn.query('COMMIT;')
conn.query('UNLOCK TABLES')
conn.query('SET foreign_key_checks=1;')
conn.query('SET unique_checks=1; ')
conn.query('SET autocommit=1;')
conn.query('ALTER TABLE %s ENABLE KEYS;' % (tablename))
end = time.time()
print "took",(end - start),"time"
print "done successfully"