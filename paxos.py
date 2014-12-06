#from datalog import DataLog
import socket
import threading
import sys
import time

IP = '127.0.0.1'
PORT = 27100
BUFFER_SIZE = 64

class Paxos:

#    dl = DataLog()
    def __init__(self, listen_port=PORT, ip_list=None, port_list=None):
        self.data = [None]*5
        self.latest_log_position = -1
        self.listen_port = listen_port
        self.ip_list = ip_list
        self.port_list = port_list
        self.ballot_num = 0
        self.accept_num = 0
        self.accept_val = (-1, -1.0) # (logposition, logentry)
        self.my_val = (-1, -1.0)
        self.majority = len(ip_list)/2 + 1
        self.ack_count = 1
        self.highest_bal = -1
        self.highest_val = (-1, -1.0)
        self.accepted_highest_bal = -1
        self.accept_count = 0
	self.max_accept = 3
	self.sent_accept_to_all = 0

    def get_prepare_response(self, bal):
        if bal >= self.ballot_num:
            self.ballot_num = bal
            reply = "ACK:" + str(bal) + ":" + str(self.accept_num) + ":" + str(self.accept_val)
        else:
            reply = "NACK:" + str(bal),":" + str(self.accept_num) + ":" + str(self.accept_val)
        return str(reply)

    def handle_ack(self, data):
        print "in Handle Ack"
        data_list = data.split(':')
        bal = int(data_list[1])
        print "self ballot number: ",self.ballot_num, bal
        if bal != self.ballot_num: # Recd older ballot ack
            return
        if data_list[0]=="NACK":
            #start new rouund
            pass
        elif data_list[0]=="ACK":
            accept_num = int(data_list[2])
            accept_val = eval(data_list[3])
            if self.highest_bal < accept_num:
                self.highest_bal = accept_num
                self.highest_val = accept_val
            self.ack_count += 1
            print "ack count ", self.ack_count, " majority", self.majority
            if(self.ack_count >= self.majority):
                print "Got majority acks! Thanks guys."
                if not(self.highest_val == (-1, -1.0)):
                    self.my_val = self.highest_val
                print "sending accept"
                msg = str("ACCEPT:" + str(self.ballot_num) + ":" + str(self.my_val))
                self.send_to_all(msg, self.ip_list, self.port_list)
		self.sent_accept_to_all = 1
                self.accept_num = self.ballot_num
                self.accept_val = self.my_val
                self.ack_count = 0
                self.accepted_highest_bal = self.accept_num
                self.accept_count = 1

        else:
            return

    def handle_accept(self, data):
        print "in HANDLE ACCEPT"
	data_list = data.split(':')
        print data, self.accepted_highest_bal
        bal = int(data_list[1])
        val = eval(data_list[2])
        if bal > self.accepted_highest_bal:
            self.accepted_highest_bal = bal
            self.accept_count = 1
        elif bal == self.accepted_highest_bal:
            self.accept_count += 1
	    print "Accept count is: ", self.accept_count
        else:
            return # Ignore!!!

        if bal >= self.ballot_num and self.sent_accept_to_all == 0:
	    self.sent_accept_to_all = 1
            print "send ACCEPT to ALL only ONCE!"
	    self.accept_num = bal
            self.accept_val = val
            self.ballot_num = bal # ?????
            msg = str("ACCEPT:" + str(bal) + ":" + str(val))
            self.send_to_all(msg, self.ip_list, self.port_list)
	
        if self.accept_count >= self.majority and self.accept_count < self.max_accept :
            print "DECIDED ON:", val
            self.data[val[0]] = val[1]
            self.latest_log_position += 1
            self.accept_num = 0
            self.ballot_num = 0
            self.accept_val = (-1, -1.0)
            self.accepted_highest_bal = -1
            print "Current Data:", self.data
	    print "\n \n"

    def req_handler(self, client_sock, addr):
        while 1:
            data = client_sock.recv(BUFFER_SIZE)
            if not data:
                break
            else:
                print "received ", data
                if data.startswith("PREPARE"):
                    msg = "Msg from server: Got ", data
                    print "sending ack ", msg
                    #self.send_single(msg, ip=self.ip_list[0], port=self.port_list[0])

                    ballot_num = int(data.split(':')[1])
                    print "ballotnum recd", ballot_num
                    resp = self.get_prepare_response(ballot_num)
                    print "prepare response:", resp
                    self.send_single(resp, ip=self.ip_list[0], port=int(data.split(':')[2]))
                    #client_sock.send(resp)
                elif data.startswith("ACK") or data.startswith("NACK"):
                    self.handle_ack(data)
                elif data.startswith("ACCEPT"):
                    self.handle_accept(data)
                elif data.startswith("DECIDE"):
                    msg = "Msg from server: Got - DECIDE"
                else:
                    msg = "Msg from server: Got data - %s" % data
        client_sock.close()

    def start_server(self):
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.bind((IP, self.listen_port))
            self.server_sock.listen(1)
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
            print "Sending ", message, " to ", ip, port
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((ip, port))
            client.send(message)
        except Exception as ex:
            print "Exception occurred in send_single: %s" % ex
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
            print "Exception occurred in send_to_all: %s" % ex
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
    print sys.argv[1]
    p = Paxos(listen_port=int(sys.argv[1]), ip_list=['127.0.0.1', '127.0.0.1'], port_list=[int(sys.argv[2]), int(sys.argv[3])])
    #thread.start_new_thread(p.start_server, ())
    tt = threading.Thread(target=p.start_server)
    tt.setDaemon(True)
    tt.start()
    #p.start_server()
#    p.send_single("PREPARE", '127.0.0.1', int(sys.argv[2]))
    #tt.join()
    while 1:
        var = raw_input("Enter Command:")
        if var.startswith("d"):
            varlist = var.split(' ')
            p.ballot_num+=1
            p.my_val = (p.latest_log_position+1, varlist[1])
            msg = str("PREPARE:"+str(p.ballot_num)+":"+str(p.listen_port))
            p.send_to_all(msg, p.ip_list, p.port_list)
            p.data
        #time.sleep(1)
except KeyboardInterrupt:
    print "Closing Server!"
    p.stop_server()
except Exception as ex:
    print "Exception occurred in start_server: %s" % ex
finally:
    # TODO: save paxos state to disk
    p.stop_server()
