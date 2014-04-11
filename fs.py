import psutil, os, sqlite3
from sys import platform as _platform

if _platform == "linux" or _platform == "linux2":
    	os.nice(1)
elif _platform == "win32":
	#set high priority
	p = psutil.Process(os.getpid())
	p.set_nice(psutil.HIGH_PRIORITY_CLASS)
	
def create_fs(db=sqlite3.connect('test'),path="testdir"):
	cur = db.cursor()
	cur.execute("PRAGMA synchronous = OFF")
	cur.execute("PRAGMA journal_mode = MEMORY")
	cur.execute("PRAGMA auto_vacuum = FULL")
	cur.execute("PRAGMA temp_store = MEMORY")
	cur.execute("PRAGMA count_changes = OFF")
	cur.execute("PRAGMA mmap_size=2335345345") 
	cur.execute("select distinct seed,tunex,tuney,amp1,amp2,turns,angle from jobparams;")
	data = cur.fetchall()
	matrix = [] 
	path = "testdir"
	for x in xrange(len(data)):
		datas = []
		datas.extend([data[x][0]])
		datas.extend(['simul'])
		a = str(data[0][1])+"_"+str(data[0][2])
		datas.extend([a])
		a = str(int(data[x][3]))+"_"+str(int(data[x][4]))
		datas.extend([a])
		datas.extend(["e"+str(data[x][5])])
		datas.extend([int(data[x][6])])
		matrix.append(datas)
	for data in matrix:
		a = os.path.sep.join([str(i) for i in data])
		os.makedirs(os.path.join(path,a))

if __name__ == "__main__":
	create_fs()