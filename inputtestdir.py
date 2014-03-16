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
	conn = sqlite3.connect('test')
	cur = conn.cursor()
except sqlite3.Error:
	print "error"
	exit(1)	
print "Opened database successfully";

#creating table
sql = "create table if not exists inputs(dir string,name string,id int,"
for i in range(1,7):
	sql += "i"+str(i)+" int,"
sql = sql[:-1] + ")"
cur.execute(sql)
sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='inputs'"
cur.execute(sql)
for row in cur:
	print row

	
#variable
value = 1
sql = 'INSERT into inputs values(?,?,?,?,?,?,?,?,?)'


#PRAGMA settings
cur.execute("PRAGMA synchronous = OFF")
cur.execute("PRAGMA journal_mode = MEMORY")
cur.execute("PRAGMA auto_vacuum = FULL")
cur.execute("PRAGMA temp_store = MEMORY")
cur.execute("PRAGMA count_changes = OFF")
cur.execute("PRAGMA mmap_size=2335345345") 

#loop for insertion of data
i = 0
j = 0
matrix = []
for dirName, subdirList, fileList in os.walk(rootDir):
	for fname in fileList:
		#if fname.endswith('.txt'): # eg: '.txt'
		if "fort.2" in fname and not fname.endswith('.gz'):
			#print fname
			with open(os.path.join(dirName, fname), "r") as FileObj:
				for lines in FileObj:
					if "NEXT" in lines:
						#print lines
						break	
					if not "SINGLE ELEMENT" in lines:
						test = [str(dirName)]
						test.extend(re.sub(r"\s+", ' ', lines).split(" "))
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
						#print "inserted"
						j = 0
						#print j
						matrix = []
	
if j:
	cur.execute("begin immediate transaction")
	cur.executemany(sql,matrix)
	conn.commit()
	#print "left inserted"
	j = 0		
	#print i
	
#commit	
#conn.commit()

#time conclude
end = time.time()
print "took",(end - start)
print "done successfully"
