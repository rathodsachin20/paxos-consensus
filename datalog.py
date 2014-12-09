import os

class DataLog:

    def __init__(self, name='log.txt'):
        self.logname = name
        self.latest_position = self.get_latest_position()

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
        f.write(str(value))
	f.write(",")
        self.latest_position = pos
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
	    for x in range(0,self.get_latest_position() - position):
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
    
    def get_empty_position_list(self):
       f = open(self.logname,'r')
       empty_pos_list = []
       current_list = self.read_data_all()
       for i in range(0,len(current_list)):
            if(current_list[i] == 'None'):
                empty_pos_list.append(i)
       print empty_pos_list
       f.close()
       return empty_pos_list

    def get_filled_dictionary(self, givelist):
        f = open(self.logname,'r')
        filled_dict = {}
        current_list = self.read_data_all()
        for val in givelist:
        	if val<len(current_list) and current_list[val] != 'None':
        		filled_dict[val] = current_list[val]
        f.close()
        print filled_dict
        return filled_dict

    def update(self, newdict):
        f = open(self.logname,'r')
        current_list = self.read_data_all()
        max_index = max(newdict.keys())
        if len(current_list) < max_index:
            for i in range(0,max_index - len(current_list)+1):
            	current_list.append('None')
        #print current_list
        for key,val in newdict.iteritems():
        	    current_list[key] = val
        print current_list
        f.close()
        fnew = open('updated_log.txt','w')
        for item in current_list:
        	fnew.write("%s,"%item)
        self.delete_log(self.logname)
        os.rename(fnew.name,self.logname)


    # Delete log
    def delete_log(self,filename):
        if os.path.isfile(filename):
	        os.remove(filename)
	else:
	    print "Log File Not Found"

val = '1111'
l = DataLog()
l.create_log()
print l.read_data_all()
L= [0]
newdict = {11:'99'}
l.update(newdict)