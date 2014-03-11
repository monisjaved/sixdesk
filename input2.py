''' TEST PROTOTYPE FOR INPUT INSERTION
	DATABASE : input (i1 - i6 string)
	AUTHOR : moonis javed
'''
import sqlite3,time
import psutil, os

#set high priority
p = psutil.Process(os.getpid())
p.set_nice(psutil.HIGH_PRIORITY_CLASS)

#time initialisation
start = time.time()

#opening connection
try:
	conn = sqlite3.connect('test')
	cur = conn.cursor()
except sqlite3.Error:
	print "error"
	exit(1)	
print "Opened database successfully";

#creating table
sql = "create table if not exists input2(id integer primary key autoincrement,"
for i in range(1,7):
	sql += "i"+str(i)+" string,"
sql = sql[:-1] + ")"
cur.execute(sql)
sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='input2'"
cur.execute(sql)
for row in cur:
	print row

	
#variable
value = 1
sql = 'INSERT into input2 (i1,i2,i3,i4,i5,i6) values(?,?,?,?,?,?)'


#PRAGMA settings
cur.execute("PRAGMA synchronous = OFF")
cur.execute("PRAGMA journal_mode = MEMORY")
cur.execute("PRAGMA auto_vacuum = FULL")
cur.execute("PRAGMA temp_store = MEMORY")
cur.execute("PRAGMA count_changes = OFF")
cur.execute("PRAGMA mmap_size=2335345345") 

#loop for insertion of data
for i in xrange(10):
	cur.execute("begin immediate transaction")
	matrix = []
	for j in xrange(10000):
		#cur.execute('INSERT into input (i1,i2,i3,i4,i5,i6) values(?,?,?,?,?,?)',(value,++value,++value,++value,++value,++value))	
		matrix.append((value,value+1,value+2,value+3,value+4,value+5))
		value += 6.001
	cur.executemany(sql,matrix)
	conn.commit()
	
#commit	
#conn.commit()

#time conclude
end = time.time()
print "took",(end - start)
print "done successfully"