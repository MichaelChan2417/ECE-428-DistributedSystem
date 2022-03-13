import sys
import socket
import time
import threading


# the class of broadcast
class broadcast(threading.Thread):
    def __init__(self, threadID, name, soc):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.soc = soc

    def run(self):
        while True:
            msg = "Hello!"
            msg = msg.encode()

            self.soc.sendto(msg, ("127.0.0.1", 5555))

            time.sleep(5)


# the class of receive end
class receive(threading.Thread):
    def __init__(self, threadID, name, soc):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.soc = soc

    def run(self):
        while True:
            msg = ""
            addr = ""
            try:
                data, addr = self.soc.recvfrom(2048)
                msg = data.decode()
            except:
                pass
            print(self.name, msg)


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


# the function to set up connections
def server_set_up(port, n_node, node_id):
    s = socket.socket()
    s.bind(("127.0.0.1", int(port)))
    s.listen(n_node)

    print(node_id, "is listening")


# the function to start connecting other nodes
def connecting(node_id, n_node, nodes_list):
    print("Start Connecting", node_id)
    # here should be a loop for connecting all the nodes
    for i in range(n_node):
        node_args = nodes_list[i].split(" ")
        print("Now Connecting to", node_args[0], "...")
        s = socket.socket()
        s.connect((node_args[1], int(node_args[2])))
        # the flag to detect whether successfully connected
        flag = 0
        while flag != 1:
            message = "CONNECT"
            s.send(message.encode())


# The main function of the node.py
def main():
    node_id, port, config_file_name = init_node()

    n_node, nodes_list = read_config_file(config_file_name)

    # server_set_up(port, n_node, node_id)

    # connecting(node_id, n_node, nodes_list)

    soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    soc.bind(("127.0.0.1", port))

    thread_broadcast = broadcast(1, node_id, soc)
    thread_receive = receive(1, node_id, soc)

    print("init success")

    thread_broadcast.start()
    thread_receive.start()


main()
