from os import times
import sys
import socket
import threading
import time

# define the upper bound when listen
n_nodes = 3
LOG = "log.txt"
EVALOG = "evaluate_log.txt"


# The filewrite function will write into the log file with the
# node name, connection time, message, its disconnect time
def fwrite(conn,start_time,size):
    while True:
        # set for the largest allowed data package
        datas = conn.recv(1024)
        if not datas:
            break
        
        contant = datas.decode()
        data = contant.split()
        # print(contant)
        size = size + len(contant)
        ## if the data's size == 2 then it is a connected log
        ## but !!! the while true will make the first send have two messages together
        ## else it should be the in-process log or a disconnected log
        if(len(data) == 5):
            # now it is a connected log
            timestamp = data[0]
            nodename = data[1]

            # the log file
            with open(LOG,"a") as f:
                f.write(timestamp + " - " + nodename + " connected\n")
            # the log_eva file
            cur_time = time.time()
            delay = cur_time - float(timestamp)
            written = "{}, {}, {}\n".format(nodename, delay, size/(cur_time-start_time))
            with open(EVALOG,"a") as f:
                f.write(written)
            #####################################################
            timestamp = data[2]
            nodename = data[3]
            message = data[4]
            with open(LOG,"a") as f:
                f.write(timestamp + " " + nodename + "\n")
                f.write(message + "\n")
            cur_time = time.time()
            delay = cur_time - float(timestamp)
            written = "{}, {}, {}\n".format(nodename, delay, size/(cur_time-start_time))
            with open(EVALOG,"a") as f:
                f.write(written)

            #######################################################
            # now the length is 3
        else:
            timestamp = data[0]
            nodename = data[1]
            message = data[2]

            cur_time = time.time()
            delay = cur_time - float(timestamp)
            written = "{}, {}, {}\n".format(nodename, delay, size/(cur_time-start_time))
            with open(EVALOG,"a") as f:
                f.write(written)

            if(message == "END"):
                # write disconnected log
                with open(LOG,"a") as f:
                    f.write(timestamp + " - " + nodename + " disconnected\n")
            else:
                with open(LOG,"a") as f:
                    f.write(timestamp + " " + nodename + "\n")
                    f.write(message + "\n")
        

    conn.close()




# the main function in logger.py to start up a server for in-come log
# uses "logger.py 8080" as arg so the port is arg[1]
def main():
    # set up for socket connection as the server with input of port
    s = socket.socket()
    port = int(sys.argv[1])
    s.bind(("127.0.0.1",port))
    # should set a number for nodes
    s.listen(n_nodes)
    # server set up successful
    print("Connection Waiting for port:",port)

    start_time = time.time()
    size = 0
    # waiting for nodes connection
    while True:
        # accept will return 2 values
        conn, addr = s.accept()

        # write log
        t=threading.Thread(target=fwrite, args=(conn,start_time,size,))
        t.start()

main()