import sys
import socket
import time


# Node.py have 3 args
# 1. Node's name; 2. IP connection address; 3. port number
def main():
    Node_Name = sys.argv[1]
    IP_d = sys.argv[2]
    port_num = int(sys.argv[3]) # ... port is an int ...

    s = socket.socket()
    s.connect((IP_d,port_num))
    
    # if successfully connect start sending messages
    time_stamp = time.time()
    data = "{} {} ".format(time_stamp, Node_Name)
    s.send(data.encode())

    while True:
        # since in generator, the data is "print", we just input the data
        income = input()
        datas = income.split()
        time_stamp = datas[0]
        message = datas[1]
        data = "{} {} {}".format(time_stamp, Node_Name, message)
        s.send(data.encode())

        # when to break and disconnect
        if(message == "END"):
            break
    
    s.close()


main()