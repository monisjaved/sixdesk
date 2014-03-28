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
sql = "create table if not exists output1(oid int NOT NULL,"
for i in range(1,61):
	sql += "o"+str(i)+" double,"
#sql = sql[:-1] + ",foreign key(oid) references input1(id) on delete cascade on update cascade)"
sql = sql[:-1] + ")"
#print sql
cur.execute(sql)
sql = "SHOW TABLES like 'input'"
cur.execute(sql)
for row in cur:
	print row
	
#variable

val = 0.01
sql = 'INSERT into output1 values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
tablename = "output1"
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
conn.query('ALTER TABLE %s DISABLE KEYS;' % (tablename))
#conn.query('START TRANSACTION;')

#loop for insertion of data
for i in xrange(100):
	#cur.execute("begin immediate transaction")
	matrix = []
	for j in xrange(1000):
		for l in xrange(1):
			#create matrix for executemany
			a=[];
			temp = i*50000+j+1
			a.extend([temp])
			for k in xrange(60):
				a.extend([val])
				val += 0.001
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
conn.query('COMMIT;')
conn.query('UNLOCK TABLES')
conn.query('SET foreign_key_checks=1;')
conn.query('SET unique_checks=1; ')
conn.query('SET autocommit=1;')
conn.query('ALTER TABLE %s ENABLE KEYS;' % (tablename))
end = time.time()
print "took",(end - start)
print "done successfully"
print