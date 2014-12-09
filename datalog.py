import os

class DataLog:

    def __init__(self, name='log.txt'):
        self.logname = name
        self.latest_position = -1

    # Create log
    def create_log(self):
        try:
	    fd = os.open(self.logname, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
	    f = os.fdopen(fd,'w')
	    f.close()
	except Exception as ex:
	    return

    # Append given data to log, and corresponding operation
    def write_data(self, value, pos):
        f = open(self.logname, 'a')
        for i in range(0, pos - self.latest_position -1):
            f.write('None,')
        self.latest_position = pos
        f.write(str(value))
	f.write(",")
	f.close()

    # Read data at given position in log and return its value with sign
    def read_data_pos(self, position):
        f = open(self.logname,'r')
	if(position > self.get_latest_position()):
	    print "Entry Does Not Exist"
	    return
	st  = f.read()
	print st.split(',')[position]
	f.close()

    def read_from_pos(self,position):
        f = open(self.logname, 'r')
	st = f.read()
	L = []
	if self.get_latest_position() - position < 0:
	    return L
	else:
	    for x in range(0,self.get_latest_position() - position)
	        L.append(st.split(',')[x+position])
	f.close()
	return L

    def get_latest_position(self):
        f = open(self.logname, 'r')
	st = f.read()
	pos =  len(st.split(',')) - 1
	f.close()
	return pos

    # Read all data and return in a list (?)
    def read_data_all(self):
        f = open(self.logname,'r')
	st = f.read()
	L = []
	for x in range(0, self.get_latest_position()):
	    L.append(st.split(',')[x])
	f.close()
	return L
	
    # Calculate current value from log (use when recovered from crash)
    def get_current_value(self):
        f = open(self.logname,'r')
        #f = open(self.logname,'r')
	st = f.read()
	total = 0.0
	for x in range(0,self.get_latest_position()):
            val = 0.0
	    strval = st.split(',')[x]
            if strval=='None':
                val = 0
            else:
                val = float(strval)
            total += val
	f.close()
	return total

    # Delete log
    def delete_log(self):
        file = "log.txt"
        if os.path.isfile(file):
	    os.remove(file)
	else:
	    print "Log File Not Found"

val = '1111'
l = DataLog(val)
l.create_log()
l.write_data(34.3, 0)
l.write_data(22, 3)
print l.read_data_all()
print l.get_current_value()
