import sys
import time
import socket

# define the upper bound when listen
n_nodes = 3

# The filewrite function will write into the log file with the
# node name, connection time, message, its disconnect time
def fwrite(conn):
    while True:
        # set for the largest allowed data package
        data = conn.recv(1024)
        if not data:
            break

        # set for the first log time
        log_cur_time =time.time()
        
        




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