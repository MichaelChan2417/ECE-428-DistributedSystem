import sys
import socket
import time
import threading
import heapq


is_received = 0
bank = {}  # store the accounts' infos
msgs = {}  # the dictionary that stores local msgs/instructions
priority_Q = []  # the priority Q contains (priority)
local_seq_number = 0  # denote the local sequence number used as the first priority in priority Q and msgs


# the class of broadcast should send out to everyone else
class broadcast(threading.Thread):
    def __init__(self, threadID, name, soc, nodes_list):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.soc = soc
        self.n_connected = len(nodes_list)
        self.nodes_list = nodes_list
        self.id = int(name[-1])

    # the helper function works as the boardcast among all the possible neighbors
    def send_all(self, msg):
        msg = msg.encode()

        for i in range(self.n_connected):
            nodes_i = self.nodes_list[i].split()
            self.soc.sendto(msg, (nodes_i[1], int(nodes_i[2])))

    def run(self):

        # The first part is that it should send to each nodes to make sure the starting-connection
        global is_received
        global bank
        global msgs
        global local_seq_number

        msg = "Hello!"
        msg = msg.encode()

        for i in range(self.n_connected):
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

        while True:
            time.sleep(10)
            # this is get the generated message
            instruction = "INSTRUCTION"
            loc_prio = local_seq_number + self.id/10
            # this means that it is the first message sending out to all, so use loc_prio as the priority judgement
            # and the third element means whether confirmed
            heapq.heappush(priority_Q, [loc_prio, instruction, 0])
            # add the instruction to local msgs
            msgs[instruction] = [loc_prio, 0]
            
            msg = instruction + "+" + str(local_seq_number) + "+" + str(self.id)
            self.send_all(msg)
            print(priority_Q)



# the class of receive end
class receive(threading.Thread):
    def __init__(self, threadID, name, soc, nodes_list):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.soc = soc
        self.n_connected = len(nodes_list)
        self.nodes_list = nodes_list
        self.id = int(name[-1])

    # the helper function works as the boardcast among all the possible neighbors
    def send_all(self, msg):
        msg = msg.encode()

        for i in range(self.n_connected):
            nodes_i = self.nodes_list[i].split()
            self.soc.sendto(msg, (nodes_i[1], int(nodes_i[2])))

    def run(self):
        global is_received
        global bank
        global msgs
        global local_seq_number

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

            # now starts dealing with instruction input
            else:
                msg_seperate = msg.split("+")
                instruction = msg_seperate[0]
                ins_prio = int(msg_seperate[1]) + int(msg_seperate[2])/10
                flag = 0  # used to detect whether confirmed

                # if it's not the first time received
                if instruction in msgs:
                    # read the last priority and judge whether the same
                    loc_prio = msgs[instruction][0]
                    # if they're same, then count+1 means it is confirmed
                    if loc_prio == ins_prio:
                        msgs[instruction][1] += 1
                        
                        # judge whether confirmed
                        if msgs[instruction][1] == self.n_connected:
                            flag = 1
                            for ele in priority_Q:
                                if ele[1] == instruction:
                                    ele[2] = 1  # confirmed
                                    break
                    else:
                        msgs[instruction] = [max(loc_prio, ins_prio), 0]
                        # update the priority in Q
                        for ele in priority_Q:
                            if ele[1] == instruction:
                                priority_Q.remove(ele)
                                heapq.heappush(priority_Q, [max(loc_prio, ins_prio), instruction, 0])
                                break
                # first time received, add it to the Q and local msgs
                else:
                    loc_prio = local_seq_number + self.id/10
                    msgs[instruction] = [max(loc_prio, ins_prio), 0]
                    heapq.heappush(priority_Q, [max(loc_prio, ins_prio), instruction, 0])

                # now we finish the msg update and we should consider whether deal with the message
                # since only the addition in confirm would lead to priority_Q's update, so in the above
                # only when flag is True, which means one of the priority has confirmed, we need to deal with the Q now
                if flag:
                    #check_Q()
                    print("Yes")
                



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

    n_nodes, nodes_list = read_config_file(config_file_name)

    soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    soc.bind(("127.0.0.1", port))
    
    global is_received
    global bank
    global msgs
    global local_seq_number

    thread_broadcast = broadcast(1, node_id, soc, nodes_list)
    thread_receive = receive(2, node_id, soc, nodes_list)
    
    thread_broadcast.start()
    thread_receive.start()


main()
