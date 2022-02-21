import sys
import time
import socket

# define the upper bound when listen
n_nodes = 3
LOG = "log.txt"


# The filewrite function will write into the log file with the
# node name, connection time, message, its disconnect time
def fwrite(conn):
    while True:
        # set for the largest allowed data package
        datas = conn.recv(1024)
        if not datas:
            break
        
        contant = datas.decode()
        data = contant.split()

        ## we have 2 conditions here
        ## if the data's size == 2 then it is a connected log
        ## else it should be the in-process log or a disconnected log
        if(len(data) == 2):
            # now it is a connected log
            timestamp = data[0]
            nodename = data[1]
            with open(LOG,"a") as f:
                f.write(timestamp + " - " + nodename + " connected\n")
            
            # now the length is 3
        else:
            timestamp = data[0]
            nodename = data[1]
            message = data[2]

            if(message == "END"):
                # write disconnected log
                with open(LOG,"a") as f:
                    f.write(timestamp + " - " + nodename + " disconnected\n")
            else:
                with open(LOG,"a") as f:
                    f.write(timestamp + " " + nodename + "\n")
                    f.write(message + "\n")




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


    # waiting for nodes connection
    while True:
        # accept will return 2 values
        conn, addr = s.accept()

        # write log
        fwrite(conn)

        conn.close()

main()