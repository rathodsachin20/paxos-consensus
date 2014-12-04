#from datalog import DataLog
import socket
import thread

IP = '127.0.0.1'
PORT = 27100
BUFFER_SIZE = 64

class Paxos:

#    dl = DataLog()

    def resp_handler(self, client_sock, addr):
        while 1:
            data = client_sock.recv(BUFFER_SIZE)
            if not data:
                break
            else:
                print data
                if data=="PREPARE":
                    msg = "Msg from server: Got PREPARE"
                elif data=="ACCEPT":
                    msg = "Msg from server: Got - ACCEPT"
                elif data=="DECIDE":
                    msg = "Msg from server: Got - DECIDE"
                else:
                    msg = "Msg from server: Got data - %s" % data
                client_sock.send(msg)
        client_sock.close()

    def start_server(self):
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.bind((IP, PORT))
            self.server_sock.listen(1)
            while 1:
                sock, addr = self.server_sock.accept()
                thread.start_new_thread(self.resp_handler, (sock, addr))
        except KeyboardInterrupt:
            print "Closing Server!"
        except Exception as ex:
            print "Exception occurred in start_server: %s" % ex
        finally:
            # TODO: save paxos state to disk
            if self.server_sock:
                self.server_sock.close()

    def send_single(self, message, ip):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((ip, PORT))
            client.send(message)
        except Exception as ex:
            print "Exception occurred in send_single: %s" % ex
        finally:
            if client:
                client.close()

    def send_to_all(self, message, iplist):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            for ip in iplist:
                client.connect((ip, PORT))
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
p = Paxos()
p.start_server()
