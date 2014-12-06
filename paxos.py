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
        self.listen_port = listen_port
        self.ip_list = ip_list
        self.port_list = port_list
        self.ballot_num = 0
        self.accept_num = 0
        self.accept_val = (-1, -1.0) # (logposition, logentry)
        self.my_val = (-1, -1.0)
        self.majority = len(ip_list)/2 + 1
        self.highest_bal = -1
        self.highet_val = (-1 -1.0)

    def get_prepare_response(self, bal):
        if bal >= self.ballot_num:
            self.ballot_num = bal
            reply = "ACK:" + str(bal) + ":" + str(self.accept_num) + ":" + str(self.accept_val)
        else:
            reply = "NACK:" + str(bal),":" + str(self.accept_num) + ":" + str(self.accept_val)
        return str(reply)

    def get_ack_reponse(data):
        data_list = data.split(':')
        bal = int(data_list[1])
        if bal != self.ballot_num: # Recd older ballot ack
            return None
        if data_list[0]=="NACK":
            #start new rouund
            pass
        else:
            accept_num = int(data_list[2])
            accept_val = eval(data_list[3])
            if self.highest_bal < accept_num:
                self.highest_bal = accept_num
                self.highest_val = accept_val
            self.ack_count += 1
            if(self.ack_count >= self.majority):
                if !self.highest_val == (-1, -1.0):
                    self.my_val = self.highest_val
                

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
                    self.send_single(resp, ip=self.ip_list[0], port=self.port_list[0])
                    #client_sock.send(resp)
                elif data.startswith("ACK") or data.startswith("NACK"):
                    self.get_ack_reponse(data)
                elif data.startswith("ACCEPT"):
                    msg = "Msg from server: Got - ACCEPT"
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

    def send_to_all(self, message, iplist, port=PORT):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            for ip in iplist:
                client.connect((ip, port))
                client.send(message)
                client.close()
        except Exception as ex:
            print "Exception occurred in send_single: %s" % ex
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
    p = Paxos(listen_port=int(sys.argv[1]), ip_list=['127.0.0.1'], port_list=[int(sys.argv[2])])
    #thread.start_new_thread(p.start_server, ())
    tt = threading.Thread(target=p.start_server)
    tt.setDaemon(True)
    tt.start()
    #p.start_server()
#    p.send_single("PREPARE", '127.0.0.1', int(sys.argv[2]))
    #tt.join()
    while 1:
        var = raw_input("Enter Command:")
        if var=="send":
            p.send_single("PREPARE:5", '127.0.0.1', int(sys.argv[2]))
        #time.sleep(1)
except KeyboardInterrupt:
    print "Closing Server!"
    p.stop_server()
except Exception as ex:
    print "Exception occurred in start_server: %s" % ex
finally:
    # TODO: save paxos state to disk
    p.stop_server()
