import os

class DataLog:
    # Create log
    def create_log(self):
        f = open('log.txt', 'w')
	f.close()

    # Append given data to log, and corresponding operation
    def write_data(self, value):
        f = open('log.txt', 'a')
	f.write(value)
	f.write(",")
	f.close()

    # Read data at given position in log and return its value with sign
    def read_data_pos(self, position):
        f = open('log.txt','r')
	if(position > self.get_latest_position()):
	    print "Entry Does Not Exist"
	    return
	str  = f.read()
	print str.split(',')[position]
	f.close()

    def read_from_pos(self,position):
        f = open('log.txt', 'r')

    def get_latest_position(self):
        f = open('log.txt', 'r')
	str = f.read()
	pos =  len(str.split(',')) - 1
	f.close()
	return pos

    # Read all data and return in a list (?)
    def read_data_all(self):
        f = open('log.txt','r')
	str = f.read()
	L = []
	for x in range(0, self.get_latest_position()):
	    L.append(int(str.split(',')[x]))
	f.close()
	return L
	
    # Calculate current value from log (use when recovered from crash)
    def get_current_value(self):
        f = open('log.txt','r')
	str = f.read()
	total = 0
	for x in range(0,self.get_latest_position()):
	    total += int(str.split(',')[x])
	f.close()
	return total

    # Delete log
    def delete_log(self):
        file = "log.txt"
        if os.path.isfile(file):
	    os.remove(file)
	else:
	    print "Log File Not Found"
    
dl = DataLog()
dl.create_log()
dl.write_data("2")
dl.write_data("-35")
dl.write_data("99")
dl.delete_log()
