from re import S
import sys
import socket
import time
import threading
import heapq
import math


is_received = 0
full_set = set()
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
        global full_set
        
        msg = "Hello!"
        msg = msg.encode()

        for i in range(self.n_connected):
            print(i)
            nodes_i = self.nodes_list[i].split()
                
            # the ith node that local one need to send to
            while True:
                self.soc.sendto(msg, (nodes_i[1], int(nodes_i[2])))
                # print("now", self.name, "is sending to", nodes_i[0])
                # wait till we got the is_received message
                time.sleep(2)
                # if the is_received is one, then it means that the start connection msg is ACKed
                if(is_received):
                    is_received = 0
                    # print(self.name, "received the ACK from", nodes_i[0])
                    break  # ready for the next node to connect
        
        # now all nodes have been connected together and we need to deal with the information

        while True:
            time.sleep(5)
            # this is get the generated message
            instruction = input()
            loc_prio = local_seq_number + self.id/10
            # this means that it is the first message sending out to all, so use loc_prio as the priority judgement
            # and the third element means whether confirmed
            heapq.heappush(priority_Q, [loc_prio, instruction, 0])
            # add the instruction to local msgs
            temp_set = set()
            temp_set.add(self.id)
            msgs[instruction] = [loc_prio, temp_set]
            
            print(self.name, "generate", instruction)
            msg = instruction + "+" + str(loc_prio) + "+" + str(self.id)
            self.send_all(msg)

            local_seq_number += 1
            # print(priority_Q)


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
        global full_set

        while True:
            flag = 0
            msg = ""
            addr = ""
            try:
                data, addr = self.soc.recvfrom(2048)
                msg = data.decode()
            except:
                pass
            
            # if the message is the first hello message, send back a ACK and turn the flag
            if msg == "Hello!":
                # print(self.name, "received Hello")
                self.soc.sendto("ACK".encode(), addr)
            # now the corresponding node received the ACK
            elif msg == "ACK":
                is_received = 1
                # print(self.name, "received ACK")

            # now starts dealing with instruction input
            else:
                msg_seperate = msg.split("+")
                instruction = msg_seperate[0]
                ins_prio = float(msg_seperate[1])
                msg_source = int(msg_seperate[2])
                
                # print(self.name, "received", msg_seperate, "from", addr)

                # if it's not the first time received
                if instruction in msgs:
                    # read the last priority and judge whether the same
                    his_prio = msgs[instruction][0]

                    # if they're same, then count+1 means it is confirmed
                    if his_prio == ins_prio:
                        msgs[instruction][1].add(msg_source)

                        # compare with the full set and send to the rest nodes
                        rest_set = full_set - msgs[instruction][1]

                        # check whether all confirmed
                        if len(rest_set) <= int((len(full_set)+1)/3):
                            temp_msg = instruction + "+" + str(ins_prio) + "+" + str(self.id)
                            self.soc.sendto(temp_msg.encode(), addr)
                            
                            flag = 1
                            for ele in priority_Q:
                                if ele[1] == instruction:
                                    ele[2] = 1
                                    break
                        else:
                            temp_msg = instruction + "+" + str(ins_prio) + "+" + str(self.id)
                            # its fake rest set
                            self.send_all(temp_msg)

                    # now the stored priority is less than the new priority
                    elif his_prio < ins_prio:
                        # make the update in local msgs and priority Q
                        for ele in priority_Q:
                            if ele[1] == instruction:
                                priority_Q.remove(ele)
                                heapq.heappush(priority_Q, [ins_prio, instruction, 0])
                        
                        temp_set = set()
                        temp_set.add(self.id)
                        temp_set.add(msg_source)
                        msgs[instruction] = [ins_prio, temp_set]
                        temp_msg = instruction + "+" + str(ins_prio) + "+" + str(self.id)
                        self.send_all(temp_msg)

                    # now the stored priority is higher than income
                    # just send back to the sender that you should take the higher one
                    else:
                        temp_msg = instruction + "+" + str(his_prio) + "+" + str(self.id)
                        self.soc.sendto(temp_msg.encode(), addr)
                
                # first time received, add it to the Q and local msgs
                # no matter what prio in other node, I should firstly tell them my position
                else:
                    # compute the new msg's priority locally
                    loc_prio = local_seq_number + self.id/10

                    temp_set = set()
                    temp_set.add(self.id)
                    msgs[instruction] = [loc_prio, temp_set]

                    heapq.heappush(priority_Q, [loc_prio, instruction, 0])
                    # now send it to all neighbors
                    temp_msg = instruction + "+" + str(loc_prio) + "+" + str(self.id)
                    self.send_all(temp_msg)

                    local_seq_number += 1

                # now we finish the msg update and we should consider whether deal with the message
                # since only the addition in confirm would lead to priority_Q's update, so in the above
                # only when flag is True, which means one of the priority has confirmed, we need to deal with the Q now
                if flag:
                    tpf = 1
                    if len(priority_Q) == 0:
                        continue
                    temp_prio_int = int(priority_Q[0][0])
                    judge_set = []
                    for ele in priority_Q:
                        if int(ele[0]) == temp_prio_int:
                            if ele[2] == 0:
                                tpf = 0
                                break
                            else:
                                judge_set.append(ele[1])
                    
                    if tpf:
                        for ele in judge_set:
                            account_modify(ele)

                            for pos in priority_Q:
                                if pos[1] == ele:
                                    priority_Q.remove(pos)
                                    break
                



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

        if (client1 not in bank) or (bank[client1] < amount):
            print("ERROR")
            exit(0)
        else:
            if client2 not in bank:
                bank[client2] = amount
                bank[client1] -= amount
            else:
                bank[client1] -= amount
                bank[client2] += amount
    
    print("BALANCES:", bank)
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
    global full_set

    for i in range(n_nodes+1):
        full_set.add(i+1)

    thread_broadcast = broadcast(1, node_id, soc, nodes_list)
    thread_receive = receive(2, node_id, soc, nodes_list)
    
    thread_broadcast.start()
    thread_receive.start()


main()
