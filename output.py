''' TEST PROTOTYPE FOR OUTPUT INSERTION
	DATABASE : output (o1 - o60 double)
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
	conn.isolation_level = "NONE"
except sqlite3.Error:
	print "error"
	exit(1)	
print "Opened database successfully";

#creating table
sql = "create table if not exists output1("
for i in range(1,61):
	sql += "o"+str(i)+" double,"
sql = sql[:-1] + ")"
cur.execute(sql)
sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='output'"
cur.execute(sql)
for row in cur:
	print row
	
#variable

val = 0.01
sql = 'INSERT into output values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

#PRAGMA settings
cur.execute("PRAGMA synchronous = OFF")
cur.execute("PRAGMA journal_mode = MEMORY")
cur.execute("PRAGMA auto_vacuum = FULL")
cur.execute("PRAGMA temp_store = MEMORY")
cur.execute("PRAGMA count_changes = OFF")
cur.execute("PRAGMA mmap_size=2335345345") 

#loop for insertion of data
for i in xrange(60):
	cur.execute("begin immediate transaction")
	matrix = []
	for j in xrange(50000):
		#create matrix for executemany
		a=[];
		for k in xrange(60):
			a.extend([val])
			val += 0.01
		matrix.append(a)
	#print matrix
	#print 'done'
	#mid = time.time()
	cur.executemany(sql,matrix)
	#print mid-start	
	conn.commit()

#print 'executed'
	
conn.commit()		
#commit	
#conn.commit()

#time conclude
#print i,j,k
end = time.time()
print "took",(end - start),"time"
print "done successfully"
print