import sqlite3,time
import psutil, os, re
from sys import platform as _platform

if _platform == "linux" or _platform == "linux2":
    	os.nice(1)
elif _platform == "win32":
	#set high priority
	p = psutil.Process(os.getpid())
	p.set_nice(psutil.HIGH_PRIORITY_CLASS)

#time initialisation
start = time.time()

#opening connection
try:
	conn = sqlite3.connect('test',isolation_level="IMMEDIATE")
	cur = conn.cursor()
except sqlite3.Error:
	print "error"
	exit(1)	
print "Opened database successfully";

#creating table
sql = "create table if not exists outputs(dir text, "
for i in range(1,61):
	sql += "o"+str(i)+" double,"
sql = sql[:-1] + ")"
cur.execute(sql)
sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='outputs'"
cur.execute(sql)
for row in cur:
	print row

	
#variable
value = 1
sql = 'INSERT into outputs values(' + ','.join(('?',)*61) + ')'

#PRAGMA settings
cur.execute("PRAGMA synchronous = OFF")
cur.execute("PRAGMA journal_mode = MEMORY")
cur.execute("PRAGMA auto_vacuum = FULL")
cur.execute("PRAGMA temp_store = MEMORY")
cur.execute("PRAGMA count_changes = OFF")
cur.execute("PRAGMA mmap_size=2335345345") 
 
# Set the directory you want to start from
rootDir = './testing'
j =0 
i = 0
matrix = []
for dirName, subdirList, fileList in os.walk(rootDir):
    #print('Found directory: %s' % dirName)
    for fname in fileList:
	if "fort.10" in fname and not fname.endswith('.gz'):
		with open (os.path.join(dirName, fname)) as FileObj:
		    for lines in FileObj:
			test = [str(dirName)]
			test.extend(re.sub(r"\s+", ' ', lines).split(" "))
			#print test
			#print len(test)
			for i in xrange(len(test)):
				if not test[i]:
					#print "empty"
					del test[i]
				if i >= len(test):
					break
			#print test
			matrix.append(test)
			j += 1
			if j == 80000:
				cur.execute("begin immediate transaction")
				cur.executemany(sql,matrix)
				conn.commit()
				print "inserted"
				j = 0
				#print i
				matrix = []

		FileObj.close()	
if j:
	cur.execute("begin immediate transaction")
	cur.executemany(sql,matrix)
	conn.commit()
	print "left inserted"
	j = 0			
			#print i
#commit	
#conn.commit()

#time conclude
end = time.time()
print "took",(end - start)
print "done successfully",j
        

