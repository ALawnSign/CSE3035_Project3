import socket
import time
import threading
import hashlib
import sys
import os

# Some global variables for readability.
TIMEOUT = 0
CRC_ERR = 1
FIREWALL = 2
ACK = 3
END = "DISCONNECT"

firewall = []                   # Firewall list that all hubs can pull from.
st_lock = threading.Lock()      # Switch table lock.
fb_lock = threading.Lock()      # Frame buffer lock.
CCS_discon = threading.Lock()   # CCS diconnect count lock.
CAS_discon = threading.Lock()   # CAS diconnect count lock.

class frame:
    def __init__(self, src, dst, redun, size, data):
        self.src = src
        self.dst = dst
        self.redun = redun
        self.size = size
        self.data = data

    def pack(self):
        # Combines the value of every notable field and turns it into bytes.
        self.redun = bytes([int(self.src) + int(self.dst) + len(self.data)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size]) + self.data.encode()
    
    def pack_time(self):
        self.size = TIMEOUT
        self.redun = bytes([int(self.src) + int(self.dst)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size]) + self.data.encode()
    
    def pack_err(self):
        self.size = CRC_ERR
        self.redun = bytes([int(self.src) + int(self.dst)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size]) + self.data.encode() 

    def pack_fire(self):
        self.size = FIREWALL
        self.redun = bytes([int(self.src) + int(self.dst)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size]) + self.data.encode()
    
    def pack_ack(self):
        self.size = ACK
        self.redun = bytes([int(self.src) + int(self.dst)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size]) + self.data.encode()
    
    def pack_discon(self):
        self.data = END
        self.redun = bytes([int(self.src) + int(self.dst) + len(self.data)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size]) + self.data.encode()

class CCS:
    def __init__ (self):
        self.host = 'localhost'
        self.port = 9999
        self.switchTable = {}
        self.discon_count = 0
    
    # Function to handle the three other hubs.
    def CCS_handle_hub(self, CAS_sock):
        time.sleep(0.01)

        with CAS_sock:
            while True:
                fire = 0
                receive = CAS_sock.recv(1024)

                if not receive:
                    time.sleep(0.01)
                    receive = CAS_sock.recv(1024)

                src = str(receive[0])
                dst = str(receive[1])
                redun = receive[2]
                size = receive[3]
                data = receive[4:4 + size].decode() if size > 0 else ''

                with st_lock:
                    self.switchTable[src[0]] = CAS_sock

                for x in firewall:
                    if x == f"{dst[0]}_#":
                        fire = 1
                        pass

                if receive[4:4 + size].decode() == END:
                    with CCS_discon:
                        self.discon_count = self.discon_count + 1

                elif self.discon_count == 3:
                    break

                elif fire != 1:
                    with st_lock:
                        send_sock = self.switchTable.get(src[0])
                    
                    if send_sock:
                        try:
                            send_sock.sendall(receive)
                        except:
                            pass
                    else:
                        with st_lock:
                            for addr, sock in self.switchTable.items():
                                if sock != CAS_sock:
                                    try:
                                        sock.sendall(receive)
                                    except:
                                        pass

    def CCS_accept(self, CCS_sock):
        while True:
            CAS_sock, addr = CCS_sock.accept()
            print(f"CAS connected from {addr}")
            # Making a persisting thread to handle hubs.
            threading.Thread(target=self.CCS_handle_hub, daemon=True, args=(CAS_sock,)).start()

    # Function to make the CCS.
    def make_CCS(self):
        CCS_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        CCS_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        CCS_sock.bind((self.host, self.port))
        # Makes a queue of 3 connections that this socket can try and accept.
        # Essentially, there are only 3 hubs that the CSS is talking to.
        CCS_sock.listen(3)
        print("CSS created on port", self.port)

        # Read firewalling stuff into the global dict.
        with open("firewall.txt", 'r') as file:
            lines = file.readlines()
            for line in lines:
                if ':' not in line:
                     continue
                # Block should be what node or CAS only accepts local data.
                # Other should be whatever else is in the thing, but should only need "block" to understand what needs to be firewalled.
                block, other = line.strip().split(':', 1)
                firewall.append(block)

        # Making a persisting thread to handle accepting connections.
        threading.Thread(target=self.CCS_accept, daemon=True, args=(CCS_sock,)).start()
        while self.discon_count != 3:
            time.sleep(0.01)

class CAS:
    def __init__(self, nodes, CAS_num):
        # Nodes would be the number of nodes that are to be created/connected to the CAS.
        self.nodes = nodes
        self.CAS_num = CAS_num
        self.host = 'localhost'
        self.port = 9998
        self.switchTable = {}
        self.frame_buffer_AS = [] # Frames from AS.
        self.discon_count = 0

    def CAS_handle_rec(self, CAS_to_CCS):
        while True:
            receive = CAS_to_CCS.recv(1024)

            while not receive:
                # Retry until something comes back.
                time.sleep(0.01)
                receive = CAS_to_CCS.recv(1024)
            
            dst = str(receive[1])
            
            with st_lock:
                send_sock = self.switchTable.get(dst)

            if send_sock:
                try:
                    time.sleep(0.01)
                    send_sock.sendall(receive)
                except:
                    pass
            else:
                with st_lock:
                    for addr, sock in self.switchTable.items():
                        try:
                            time.sleep(0.01)
                            sock.sendall(receive)
                        except:
                            pass
            
            # Teardown condition.
            if not self.frame_buffer_AS and self.discon_count == self.nodes:
                break


    def CAS_handle_CCS(self, CAS_to_CCS):
        threading.Thread(target=self.CAS_handle_rec, daemon=True, args=(CAS_to_CCS,)).start()

        while True:
            if not self.frame_buffer_AS:
                pass
            else:
                # Frame buffer is a FIFO.
                with fb_lock:
                    fr = self.frame_buffer_AS.pop() # Take frame from AS buffer to send to CCS.

                CAS_to_CCS.sendall(fr)
                time.sleep(0.01)
                

            # Exit and cleanup scenario.
            if not self.frame_buffer_AS and self.discon_count == self.nodes:
                # Send a disconnect frame to the CCS.
                src = str(("CAS"))
                dst = str(("CCS"))
                fr = frame(src, dst, 0, len(END), '')
                mes = fr.pack_discon
                CAS_to_CCS.sendall(mes)
                break


    def CAS_handle_AS(self, AS_sock):
        while True:
            # Var to keep track of whether or not we've sent something.
            sent = 0

            receive = AS_sock.recv(1024)

            if not receive:
                # Retry once.
                time.sleep(0.01)
                receive = AS_sock.recv(1024)

            if not receive:
                # Send timeout frame...
                fr = frame(dst, src, 0, 0, '')
                mes = fr.pack_time()
                AS_sock.sendall(mes)
                
            else:
                #... or continue and do checks.
                src = str(receive[0])
                dst = str(receive[1])
                redun = int(receive[2])
                size = receive[3]
                data = receive[4:4 + size].decode() if size > 0 else ''


                with st_lock:
                    self.switchTable[src] = AS_sock

                # Checking if node is within network.
                for addr, sock in self.switchTable.items():
                    if addr == dst:
                        with st_lock:
                            sending_sock = self.switchTable[addr]
                        sending_sock.sendall(receive)
                        sent = 1

                for x in firewall:
                    if x == dst and sent != 1:
                        fr = frame(dst, src, 0, 0, '')
                        mes = fr.pack_fire()
                        AS_sock.sendall(mes)  

                if receive[4:4 + size].decode() == END:
                    with CAS_discon:
                        self.discon_count = self.discon_count + 1

                elif redun != int(src) + int(dst) + len(data):
                    fr = frame(dst, src, 0, 0, '')
                    mes = fr.pack_err()
                    AS_sock.sendall(mes)

                elif self.discon_count == self.nodes and not self.frame_buffer_AS:
                    break

                else:
                    # If all checks are okay, put it in the AS buffer.
                    with fb_lock:
                        self.frame_buffer_AS.append(receive)
    
    def CAS_accept(self, CAS_sock):
        while True:
            AS_sock, addr = CAS_sock.accept()
            print(f"AS connected from {addr}")
            # Making a persisting thread to handle hubs.
            threading.Thread(target=self.CAS_handle_AS, daemon=True, args=(AS_sock,)).start()

    def make_CAS(self, net_num, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as CAS_to_CCS:
            while True:
                try:
                    # Connect to the CCS on port 9999.
                    CAS_to_CCS.connect(('localhost', 9999))
                    threading.Thread(target=self.CAS_handle_CCS, daemon=True, args=(CAS_to_CCS,)).start()
                    break
                except:
                    time.sleep(0.01) 
            
            CAS_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            CAS_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Open several ports.
            CAS_sock.bind((self.host, port))
            CAS_sock.listen(self.nodes + 1)
            print("CAS created on port", port)

            threading.Thread(target=self.CAS_accept, daemon=True, args=(CAS_sock,)).start()
            time.sleep(0.01)
            for x in range(self.nodes):
                n = AS(x + 1)
                threading.Thread(target=n.make_AS, daemon=True, args=(net_num, x + 1, port)).start()

            # Keeps port alive until teardown.
            while True:
                time.sleep(0.01)

class AS:
    def __init__(self, node_desig):
        self.node_desig = node_desig

    def receive_data(self, AS_sock, outpath):
        with open(outpath, 'a') as out:
            while True:
                try:
                    rec = AS_sock.recv(1024)
                    
                    if not rec:
                        # Retry once.
                        time.sleep(0.01)
                        rec = AS_sock.recv(1024)

                    src = str(rec[0])
                    dst = str(rec[1])
                    red = rec[2]
                    size = rec[3]
                    data = rec[4:4 + size].decode() if size > 0 else ''
                    out.write(f"{src[0]}_{src[1]}:{data}\n")
                    out.flush()
                except:
                    break


    def make_AS(self, net_num, phys_num, port):
        path = f"node{net_num}_{phys_num}.txt"
        outpath = f"node{net_num}_{phys_num}output.txt"
        node_desig = f"{net_num}_{phys_num}"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as AS_sock:
            while True:
                try:
                    AS_sock.connect(('localhost', port))
                    break
                except:
                    time.sleep(0.01)
            
            threading.Thread(target=self.receive_data, daemon=True, args=(AS_sock, outpath)).start()

            if not os.path.exists(path):
                print(f"{path} not found, skipping AS {net_num}_{phys_num}")
                # Since there is no file, immediately send disconnect frame.
                dst = "8"
                fr = frame(node_desig, dst, 0, 0, '')
                mes = fr.pack_discon()
                AS_sock.sendall(mes)
                return

            with open(path, 'r') as file:
                lines = file.readlines()

                for line in lines:
                    if ':' not in line:
                        continue
                    dst, msg = line.strip().split(':', 1)
                    msg = msg.strip()
                    data_bytes = msg.encode()
                    fr = frame(node_desig, dst.strip(), 0, len(data_bytes), msg)
                    time.sleep(0.01)
                    AS_sock.sendall(fr.pack())

                    # Waiting for some kind of response.
                    time.sleep(0.01)
                    receive = AS_sock.recv(1024)

                    while not receive:
                        # Retry until something comes back.
                        time.sleep(0.01)
                        receive = AS_sock.recv(1024)

                    size_ack = receive[3]
                    
                    if int(size_ack) == TIMEOUT:
                        # Resending.
                        AS_sock.sendall(fr)

                    elif int(size_ack) == CRC_ERR:
                        AS_sock.sendall(fr)

                    elif int(size_ack) == FIREWALL:
                        pass

                    elif int(size_ack) == ACK:
                        pass
            
            
            # Done with sending messages, so send diconnect frame.
            fr = frame(node_desig, dst, 0, 0, '')
            mes = fr.pack_discon()
            AS_sock.sendall(mes)
            while True:
                pass


class main:
    # Be sure to add the nodes variable back.
    def __init__(self, nodes):
        self.nodes = nodes

        cc = CCS()
        threading.Thread(name="CCS", target=cc.make_CCS, daemon=True).start()

        ca1 = CAS(nodes, 1)
        threading.Thread(name="CAS1", target=ca1.make_CAS, daemon=True, args=(1, 9998,)).start()

        ca2 = CAS(nodes, 2)
        threading.Thread(name="CAS2", target=ca2.make_CAS, daemon=True, args=(2, 9997,)).start()

        ca3 = CAS(nodes, 3)
        threading.Thread(name="CAS3", target=ca3.make_CAS, daemon=True, args=(3, 9996)).start()
        time.sleep(3)