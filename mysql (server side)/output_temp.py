import sqlite3,time
import psutil, os, MySQLdb, sys
from config import *

#set high priority
p = psutil.Process(os.getpid())
p.set_nice(psutil.HIGH_PRIORITY_CLASS)

#time initialisation
start = time.time()

#opening connection
try:
	conn = MySQLdb.connect(host,user,password,db)
	conn.autocommit(False)
	cur = conn.cursor()
except MySQLdb.Error:
	print "error"
	exit(1)	
print "Opened database successfully";

#creating table
sql = "create table if not exists output("
for i in range(1,61):
	sql += "o"+str(i)+" double,"
sql = sql[:-1] + ")"
cur.execute(sql)
sql = "SHOW TABLES like 'output'"
cur.execute(sql)
for row in cur:
	print row

#creating temp file for data (WRITE ONLY MODE)
fo = open("fo.txt","w")
	
#variable
val = 0.01
sql = 'INSERT into output1 values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
tablename = "output"

conn.query('SET autocommit=0;')
conn.query('SET unique_checks=0; ')
conn.query('SET foreign_key_checks=0;')
conn.query('LOCK TABLES %s WRITE;' % (tablename))
conn.query('ALTER TABLE %s DISABLE KEYS;' % (tablename))

#loop for generation of test data
for i in xrange(10):
	matrix = []
	for j in xrange(10000):
		for l in xrange(1):
			#create matrix for executemany
			a=[];
			for k in xrange(60):
				a.extend([val])
				val += 0.001
			matrix.append(a)
	for row in matrix:
		print >>fo, tuple(row)

fo.close()

#load data into table
cur.execute("LOAD DATA LOCAL INFILE 'fo.txt' INTO TABLE output FIELDS TERMINATED BY ',' LINES STARTING BY '(' TERMINATED BY ')';")
conn.commit()		

conn.query('COMMIT;')
conn.query('UNLOCK TABLES')
conn.query('SET foreign_key_checks=1;')
conn.query('SET unique_checks=1; ')
conn.query('SET autocommit=1;')
conn.query('ALTER TABLE %s ENABLE KEYS;' % (tablename))
os.remove("fo.txt")

#time conclude
end = time.time()
print "took",(end - start)
print "done successfully"
print