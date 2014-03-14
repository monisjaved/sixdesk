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
sql = "create table if not exists inputs(file string,name string,id int,"
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
dir = "fort2"
list_dir = []
list_dir = os.listdir(dir)
list_dir.sort()
#print list_dir
for file in list_dir:
	#if file.endswith('.txt'): # eg: '.txt'
	if "fort.2" in file:
		#print file
		i = 0
		matrix = []
		with open(os.path.join(dir, file), "r") as FileObj:
		    for lines in FileObj:
			if "NEXT" in lines:
				#print lines
				break	
			if not "SINGLE ELEMENT" in lines:
				test = [str(file)]
				test.extend(re.sub(r"\s+", ' ', lines).split(" "))
				if len(test) >= 9:
					for k in xrange(len(test)-9):
						#print k
						del test[len(test)-1]
				#print test
				matrix.append(test)
				i += 1
				if i == 10000:
					cur.execute("begin immediate transaction")
					cur.executemany(sql,matrix)
					conn.commit()
					i = 0
					#print i
					matrix = []
	
		if i > 0:
			cur.execute("begin immediate transaction")
			cur.executemany(sql,matrix)
			conn.commit()
			i = 0
			matrix = []			
		#print i
#commit	
#conn.commit()

#time conclude
end = time.time()
print "took",(end - start)
print "done successfully"
