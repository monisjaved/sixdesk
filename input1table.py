''' TEST PROTOTYPE FOR INPUT AND OUTPUT INSERTION 
	FLATDATABASE : common (i1 - i6 int, o1-o60 double)
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
	conn = sqlite3.connect("test")
	cur = conn.cursor()
	conn.isolation_level = "NONE"
except sqlite3.Error:
	print "error"
	exit(1)
print "Opened database successfully";

sql = "create table if not exists common(id int NOT NULL ,"
for i in range(1,7):
	sql += "i"+str(i)+" int,"
for i in range(1,61):
	sql += "o"+str(i)+" double,"
sql  += "primary key(id,i1,o1))"
cur.execute(sql)
	
#variable
val = 1
value = 1
sql = "INSERT into common values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

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
		a = []
		temp = i*10000+j+1
		a.extend([temp])
		b = ""
		for k in xrange(6):
			a.extend([value])
			value += 1
		for l in xrange(30):
			c = []
			c.extend(a)
			for k in xrange(60):
				c.extend([val])
				val += 0.01
			matrix.append(c);
			#cur.execute('INSERT into input (i1,i2,i3,i4,i5,i6) values(?,?,?,?,?,?)',(value,++value,++value,++value,++value,++value))
	cur.executemany(sql,matrix)
	conn.commit()
		
#conn.commit()	
#commit	
#conn.commit()

#time conclude
print i,j,k
end = time.time()
print "took",(end - start),"time"
print "done successfully"