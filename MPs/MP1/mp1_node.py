from glob import glob
from operator import is_
from pprint import isrecursive
import sys
import socket
import time
import threading
from tokenize import blank_re

is_received = 0
bank = {}

# the class of broadcast should send out to everyone else
class broadcast(threading.Thread):
    def __init__(self, threadID, name, soc, nodes_list):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.soc = soc
        self.nodes_list = nodes_list

    def run(self):

        # The first part is that it should send to each nodes to make sure the starting-connection
        global is_received

        msg = "Hello!"
        msg = msg.encode()

        for i in range(len(self.nodes_list)):
            nodes_i = self.nodes_list[i].split()
                
            # the ith node that local one need to send to
            while True:
                self.soc.sendto(msg, (nodes_i[1], int(nodes_i[2])))
                print("now", self.name, "is sending to", nodes_i[0])
                # wait till we got the is_received message
                time.sleep(5)
                # if the is_received is one, then it means that the start connection msg is ACKed
                if(is_received):
                    is_received = 0
                    print(self.name, "received the ACK from", nodes_i[0])
                    break  # ready for the next node to connect
        
        # now all nodes have been connected together and we need to deal with the information



# the class of receive end
class receive(threading.Thread):
    def __init__(self, threadID, name, soc):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.soc = soc

    def run(self):
        global is_received

        while True:
            msg = ""
            addr = ""
            try:
                data, addr = self.soc.recvfrom(2048)
                msg = data.decode()
            except:
                pass
            
            # if the message is the first hello message, send back a ACK and turn the flag
            if msg == "Hello!":
                print(self.name, "received Hello")
                self.soc.sendto("ACK".encode(), addr)
            # now the corresponding node received the ACK
            elif msg == "ACK":
                is_received = 1
                print(self.name, "received ACK")

# the function dealing with input
def init_node():
    node_id = sys.argv[1]
    port = sys.argv[2]
    config_file_name = sys.argv[3]
    return node_id, int(port), config_file_name


# The function used to read the configuration file for each node_server
def read_config_file(config_file_name):
    cont = []
    with open(config_file_name, 'r') as f:
        # first, get the total number of node that it must connect to
        line = f.readline()
        n_node = int(line)

        # then for each target, write them into the cont that to be output
        line = f.readline()
        while line:
            line = line.strip("\n")
            cont.append(line)
            line = f.readline()

    return n_node, cont


# the function when dealing with the input from gentx
# with a dictionary outside containing the total informations
# where message is the income string; while bank is the dictionary that store all the infos.
def account_modify(message):
    global bank

    info = message.split()

    # it means that it is a deposit
    if len(info) == 3:
        bank[info[1]] = int(info[2])
    # else it would be transfer
    elif len(info) == 5:
        client1 = info[1]
        client2 = info[3]
        amount = int(info[4])
        
    
    return


# The main function of the node.py
def main():
    node_id, port, config_file_name = init_node()

    n_node, nodes_list = read_config_file(config_file_name)

    print(nodes_list)

    soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    soc.bind(("127.0.0.1", port))
    
    global is_received

    thread_broadcast = broadcast(1, node_id, soc, nodes_list)
    thread_receive = receive(2, node_id, soc)
    
    thread_broadcast.start()
    thread_receive.start()


main()
