#from datalog import DataLog
import socket
import threading
import sys
import time

#IP = '172.30.0.49'
#IP = '127.0.0.1'
PORT = 27100
BUFFER_SIZE = 64

class Paxos:

#    dl = DataLog()
    def __init__(self, localip, myip, listen_port, ip_list, port_list, def_ballot):
        self.data = [None]*40
        self.latest_log_position = -1
        self.localip = localip
        self.ip = myip #
        self.listen_port = listen_port
        self.ip_list = ip_list
        self.port_list = port_list
	self.def_ballot = def_ballot
        self.ballot_num = def_ballot
        self.accept_num = 0
        self.accept_val = (-1, -1.0, self.ip+";"+str(listen_port)) # (logposition, logentry)
        self.my_val = (-1, -1.0, self.ip+";"+str(listen_port))
        self.latest_decided_val = (-1, -1.0, '')
        #self.my_oldval = (-1, -1.0)
        self.majority = len(ip_list)/2 + 1
        self.ack_count = 0
        self.highest_accept_num = -1
        self.highest_accept_val = (-1, -1.0, '')
        self.accepted_highest_bal = -1
        self.accept_count = 0
        self.recd_maj_acks = False
        self.decided = False
        self.decide_lock = threading.Lock()
        self.accept_lock = threading.Lock()
        #self.log_pos_lock = threading.Lock()
        self.my_success = threading.Event()
        self.my_success.clear()
        self.my_success1 = threading.Event()
        self.my_success1.clear()
        self.state = 0 # 0-initial, 1-sent preapre/waiting for acks, 2-got acks, waiting for accepts, 3-got myvalue accepted, 4-failed/got someone else value accepted

    def prepare(self, val):
        #if self.my_oldval != (-1, -1.0):
        #    return None
        self.ack_count = 1
        self.highest_accept_num = -1
        self.highest_accept_val = (-1, -1.0, '')
        self.my_val = val
        self.my_success.clear()
        self.my_success1.clear()
        #self.ballot_num += 1
        self.ballot_num += self.def_ballot+1
        self.recd_maj_acks = False
        self.decided = False
        msg = str("PREPARE:"+str(self.ballot_num)+":"+self.ip+";"+str(p.listen_port))
        print "SENDING PREPARE from node ", self.listen_port%5
        self.send_to_all(msg, self.ip_list, self.port_list)
        self.state = 1

    def get_prepare_response(self, bal):
        if bal > self.ballot_num:
            self.ballot_num = bal
            reply = "ACK:" + str(bal) + ":" + str(self.accept_num) + ":" + str(self.accept_val)
        else:
            reply = "NACK:" + str(bal),":" + str(self.accept_num) + ":" + str(self.accept_val)
        return str(reply)

    def handle_ack(self, data):
        try:
             print "in Handle Ack"
             data_list = data.split(':')
             bal = int(data_list[1])
             #print "self ballot number: ",self.ballot_num, bal
             if bal != self.ballot_num: # Recd older ballot ack
                 return
             if data_list[0]=="NACK":
                 #start new rouund
                 self.state = 4
                 self.my_success.set()
                 print "got NACK in thread", self.listen_port%5
                 return
             elif data_list[0]=="ACK":
                 accept_num = int(data_list[2])
                 accept_val = eval(data_list[3])
                 if self.highest_accept_num < accept_num:
                     self.highest_accept_num = accept_num
                     self.highest_accept_val = accept_val
                 self.ack_count += 1
                 with self.accept_lock:
                     print "ack count ", self.ack_count, " majority", self.majority
                     #print self.ack_count==self.majority
                     if(self.ack_count >= self.majority and not self.recd_maj_acks):
                         print "Got majority acks! Thanks guys."
                         self.recd_maj_acks = True
                         if not(self.highest_accept_val[0] == -1):
                             #self.my_oldval = self.my_val
                             self.my_val = self.highest_accept_val
                         #print "sending accept"
                         msg = str("ACCEPT:" + str(self.ballot_num) + ":" + str(self.my_val))
                         self.send_to_all(msg, self.ip_list, self.port_list)
                         self.accept_count = 1
                         #self.sent_accept_to_all = 1
                         self.accept_num = self.ballot_num
                         self.accept_val = self.my_val
                         self.state = 2
                    #     self.accepted_highest_bal = self.accept_num
                         #self.ack_count = 1

             else:
                 return
        except Exception as ex:
            print "Exception in handle_ack", ex

    def handle_accept(self, data):
        try:
            #print "in HANDLE ACCEPT"
            data_list = data.split(':')
            #print data, self.accept_num
            bal = int(data_list[1])
            val = eval(data_list[2])
            #if self.state==3 or self.state==4:
            #    return
            if val[0] <= self.latest_decided_val[0]:
                return
            if self.state==1 and bal > self.my_val[0]:
                self.state = 6
                self.my_success.set()
            if bal == self.accept_num:
                self.accept_count += 1
                #print "Accept count is: ", self.accept_count
            elif bal > self.accept_num or self.state==1:
                self.accept_num = bal
                self.state = 6
                self.my_success.set()
                #if self.ballot_num < self.accept_num:
                #    self.ballot_num = self.accept_num
                self.accept_val = val
                self.accept_count = 2
                self.decided = False
                msg = str("ACCEPT:" + str(bal) + ":" + str(val))
                self.send_to_all(msg, self.ip_list, self.port_list)
            else:
                print "at server", self.listen_port, "got bal", bal, " state=", self.state, "val", self.accept_val
                return # Ignore!!!

            '''if bal >= self.ballot_num and self.sent_accept_to_all == 0:
                self.sent_accept_to_all = 1
                print "send ACCEPT to ALL only ONCE!"
                self.accept_num = bal
                self.accept_val = val
                self.ballot_num = bal # ?????
                msg = str("ACCEPT:" + str(bal) + ":" + str(val))
                self.send_to_all(msg, self.ip_list, self.port_list)
            '''
            with self.decide_lock:
                #print "decided", self.decided
                if self.accept_count >= self.majority and not self.decided:
                    self.decide(bal, val)
        except Exception as ex:
            print "Exception in handle_accept:", ex

    def decide(self, bal, val):
        try:
            print "DECIDED ON:", val
            self.decided = True
            self.data[val[0]] = val[1]
            self.latest_log_position = val[0]
            #self.latest_log_position += 1
            self.latest_decided_val = val
            self.accept_num = 0
            self.accept_val = (-1, -1.0, '')
            self.ballot_num = self.def_ballot
            self.accept_count = 0
            if self.my_val==val:
                self.state = 3
            else:
                self.state = 4
                # retry in next round
            self.my_success.set()
            self.my_success1.set()
            #msg = str("DECIDE:" + str(bal) + ":" + str(val))
            #self.send_to_all(msg, self.ip_list, self.port_list)
            #print "Current Data:", self.data
            print "\n \n"
        except Exception as ex:
            print "Exception occurred in decide ", ex

    def handle_decide(self, data):
        try:
            bal = int(data.split(':')[1])
            val = eval(data.split(':')[2])
            if self.latest_log_position == val[0]:
                return
            else:
                #print "DECIDED ON:", val
                self.decided = True
                self.data[val[0]] = val[1]
                self.latest_log_position = val[0]
                #self.latest_log_position += 1
                self.latest_decided_val = val
                self.accept_num = 0
                self.accept_val = (-1, -1.0, '')
                #self.ballot_num = self.listen_port%5+1
                self.accept_count = 0
                if not self.my_val==val:
                    self.state = 4
                    # retry in next round
                else:
                    self.state = 3
                self.my_success.set()
        except Exception as ex:    
            print "Exception occurred in handle_decide ", ex

    def req_handler(self, client_sock, addr):
        try:
            while 1:
                data = client_sock.recv(BUFFER_SIZE)
                if not data:
                    break
                else:
                    #print "received ", data
                    if data.startswith("PREPARE"):
                        #print "sending ack ", msg
                        #self.send_single(msg, ip=self.ip_list[0], port=self.port_list[0])
			data_list = data.split(':')
			#print "Datalist prepare", data_list
                        ballot_num = int(data_list[1])
                        #print "ballotnum recd", ballot_num, " my ballot_num", self.ballot_num
                        resp = self.get_prepare_response(ballot_num)
                        #print "prepare response:", resp
                        self.send_single(resp, ip=data_list[2].split(';')[0], port=int(data_list[2].split(';')[1]))
                        #client_sock.send(resp)
                    elif data.startswith("ACK") or data.startswith("NACK"):
                        self.handle_ack(data)
                    elif data.startswith("ACCEPT"):
                        self.handle_accept(data)
                    elif data.startswith("DECIDE"):
                        #self.handle_decide(data)
                        msg = "Msg from server: Got - DECIDE"
                    else:
                        msg = "Msg from server: Got data - %s" % data
            client_sock.close()
        except Exception as ex:
            print "Exception in req_handler:", ex

    def start_server(self):
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #self.server_sock.bind((IP, self.listen_port))
            self.server_sock.bind((self.localip, self.listen_port))
            self.server_sock.listen(1)
            print "starting server on", self.localip
            while 1:
                sock, addr = self.server_sock.accept()
                #thread.start_new_thread(self.resp_handler, (sock, addr))
                t = threading.Thread(target=self.req_handler, args=(sock, addr))
                t.start()
        except KeyboardInterrupt:
            print "Closing Server!"
        except Exception as ex:
            print "Exception occurred in start_server: %s" % ex
        finally:
            # TODO: save paxos state to disk
            if self.server_sock:
                self.server_sock.close()

    def stop_server(self):
        if self.server_sock:
            self.server_sock.close()

    def send_single(self, message, ip, port=PORT):
        try:
            #print "Sending ", message, " to ", ip, port
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((ip, port))
            client.send(message)
        except Exception as ex:
            print "Exception occurred in send_single: %s %s" % (ex, ip)
        finally:
            if client:
                client.close()

    def send_to_all(self, message, ip_list, port_list):
        try:
            for ip, port in zip(ip_list, port_list):
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect((ip, port))
                client.send(message)
                client.close()
        except Exception as ex:
            print "Exception occurred in send_to_all: %s %s" % (ex, ip)
        finally:
            if client:
                client.close()

"""
    # Initialize log
    def init_system():

    def update_log():

    def deposit(value):

    def withdraw(value):

    def balance():

    def get_log_position():

    def broadcast(value):

    def recover():
"""

try:
    ipfile = sys.argv[1]
    #ipfile = 'iplist'
    with open(ipfile) as f:
        content = f.readlines()
    ipport = [i.split('\n',1)[0] for i in content]
    ip_list = []
    port_list = []

    with open('config') as f:
        conf = f.readlines()
    def_ballot = int(conf[0])
    def_ballot = int(sys.argv[2]) #TODO: remove this
    print "default ballot:", def_ballot

    for x in ipport:
	ip_list.append(x.split(':')[0])
	port_list.append(int(x.split(':')[1]))
    print "iplst:", ip_list, "ports:", port_list
    p = Paxos(ip_list[0], ip_list[1], port_list[0], ip_list=ip_list[2:], port_list=port_list[2:], def_ballot=def_ballot)
    #thread.start_new_thread(p.start_server, ())
    tt = threading.Thread(target=p.start_server)
    tt.setDaemon(True)
    tt.start()
    #p.start_server()
#    p.send_single("PREPARE", '127.0.0.1', int(sys.argv[2]))
    #tt.join()
    time.sleep(3)
    while 1:
        var = raw_input("Enter Command:")
        #var = "a"
        varlist = var.split(' ')
        if var.startswith("d"):
            #p.ballot_num+=1
            val = (p.latest_log_position+1, varlist[1], p.ip)
            #msg = str("PREPARE:"+str(p.ballot_num)+":"+str(p.listen_port))
            #p.send_to_all(msg, p.ip_list, p.port_list)
            p.prepare(val)
            time.sleep(3)
            if p.state==3:
                print "--->>>>>>>>>------>>>>>>>------->>>>------SUCCESS!!!   node:",p.ip,p.listen_port%5, val
            else:
                print "---<<<<<---------<<<<<<<<--------<<<<-----FAILED!! PLEASE RETRY!!!    node:",p.ip,p.listen_port%5, val
            p.state=0
            print p.data
        elif var.startswith('p'):
	    print p.data
        elif var.startswith("a"):
            port = int(port_list[0])
            for i in xrange(5):
                val = (p.latest_log_position+1, p.def_ballot*1000+i, p.ip)
                p.prepare(val)
                #p.my_success1.wait()
                #p.my_success1.clear()
                time.sleep(5)
                if p.state==3:
                    print "--->>>>>>>>>------>>>>>>>------->>>>------SUCCESS!!!   node:",p.ip,p.listen_port%5, val
                else:
                    print "---<<<<<---------<<<<<<<<--------<<<<-----FAILED!! PLEASE RETRY!!!    node:",p.ip,p.listen_port%5, val
                p.state=0
                print "DATA AT SERVER ", port%5, "==========> ", p.data
            break
        #time.sleep(1)
except KeyboardInterrupt:
    print "Closing Server!"
    if p:
	p.stop_server()
except Exception as ex:
    print "Exception occurred in start_server: %s" % ex
finally:
    # TODO: save paxos state to disk
    if p:
	p.stop_server()
