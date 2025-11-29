import socket
import time
import threading
import sys
import os

# TODO: Working on handle_CSS

# Some global variables for readability.
TIMEOUT = 0
CRC_ERR = 1
FIREWALL = 2
ACK = 3
END = "DISCONNECT"

firewall = []               # Firewall list that all hubs can pull from.
st_lock = threading.Lock()  # Switch table lock.
fb_lock = threading.Lock()  # Frame buffer lock.
CCS_discon = threading.Lock()
CAS_discon = threading.Lock()

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
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size])    
    
    def pack_err(self):
        self.size = CRC_ERR
        self.redun = bytes([int(self.src) + int(self.dst)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size])    

    def pack_fire(self):
        self.size = FIREWALL
        self.redun = bytes([int(self.src) + int(self.dst)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size])
    
    def pack_ack(self):
        self.size = ACK
        self.redun = bytes([int(self.src) + int(self.dst)])
        return bytes([int(self.src)]) + bytes([int(self.dst)]) + self.redun + bytes([self.size])

class CCS:
    def __init__ (self):
        self.host = 'localhost'
        self.port = 9999
        self.switchTable = {}
        self.frame_buffer = []
        self.discon_count = 0
    
    # Function to handle the three other hubs.
    def CCS_handle_hub(self, CAS_sock):
        time.sleep(0.01)

        with CAS_sock:
            while True:
                receive = CAS_sock.recv(1024)
                src = str(receive[0])
                dst = str(receive[1])
                redun = receive[2]
                size = receive[3]
                data = receive[4:4 + size].decode() if size > 0 else ''

                with st_lock:
                    self.switchTable[src[0]] = CAS_sock

                for x in firewall:
                    if f"{x[0]}_#" == f"{dst[0]}_#":
                        fr = frame("CCS", src, 0, 0, '')
                        mes = fr.pack_fire()
                        CAS_sock.sendall(mes)  
                        break

                if receive[4:4 + size].decode() == END:
                    with CCS_discon:
                        self.discon_count = self.discon_count + 1
                elif not receive:
                    fr = frame("CCS", src, 0, 0, '')
                    mes = fr.pack_time()
                    CAS_sock.sendall(mes)
                elif redun != bytes([int(src) + int(dst)]):
                    fr = frame("CCS", src, 0, 0, '')
                    mes = fr.pack_err()
                    CAS_sock.sendall(mes)
                elif self.discon_count == 3:
                    break
                else:
                    with st_lock:
                        send_sock = self.switchTable.get[src[0]]
                    
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
        return 0

class CAS:
    def __init__(self, nodes):
        # Nodes would be the number of nodes that are to be created/connected to the CAS.
        self.nodes = nodes
        self.host = 'localhost'
        self.port = 9998
        self.switchTable = {}
        self.frame_buffer = []
        self.discon_count = 0

    def receive_data(socket):
        return 0

    def handle_CCS(self, socket):
        while True:
            if not self.frame_buffer:
                time.sleep(0.01)                
            else:
                # Frame buffer is a FIFO.
                with fb_lock:
                    fr = self.frame_buffer.pop()
                size = fr[3]

                if fr[4:4 + size] == END:
                    with CAS_discon:
                        self.discon_count = self.discon_count + 1
                else:
                    socket.sendall(fr)
                    time.sleep(0.01)

                    receive = socket.recv(1024)
                    src = receive[0]
                    size = receive[3]

                    if not receive:
                        socket.sendall(fr)
                    elif int(size) == CRC_ERR:
                        fr = frame("CAS", src, 0, 0, '')
                        mes = fr.pack_err()
                        with st_lock:
                            source_sock = self.switchTable.get(src)
                        source_sock.sendall(mes)
                    elif # Blah blah, the other errors.
            
            # Exit and cleanup scenario.
            if not self.frame_buffer and self.discon_count == self.nodes:
                break


    def handle_AS(self):
        return 0

    def make_CAS(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as CAS_to_CSS:
            while True:
                try:
                    # Connect to the CCS on port 9999.
                    CAS_to_CSS.connect(('localhost', 9999))
                    threading.Thread(target=self.handle_CCS, daemon=True, args=(CAS_to_CSS,)).start()
                    break
                except:
                    time.sleep(0.1) 
                
            CCS_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            CCS_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            CCS_sock.bind((self.host, self.port))  
            
            for x in range(self.nodes):
                # Need to make the AS class, but you get the point.
                n = AS(x + 1)
                threading.Thread(target=n.make_AS, daemon=True, args=(x + 1,)).start()

            threading.Thread(target=self.handle_AS, daemon=True, args=(CAS_to_CSS,)).start()

class main:
    # Be sure to add the nodes variable back.
    def __init__(self, nodes):
        self.nodes = nodes

        # All below is just some dumb test stuff.
        mes = "Blah blah blah"
        fr = frame("1_2", "2_4", 0, len(mes.encode()), mes)
        a = fr.pack()
        size = a[3]
        b = a[4:4 + size].decode()
        print(a)
        print(b)
        # End of dumb test stuff.

        cc = CCS()
        threading.Thread(name="CCS", target=cc.make_CCS, daemon=True).start()

        ca1 = CAS(nodes)
        threading.Thread(name="CAS1", target=ca1.make_CAS, daemon=True).start()

        ca2 = CAS(nodes)
        threading.Thread(name="CAS2", target=ca2.make_CAS, daemon=True).start()

        ca3 = CAS(nodes)
        threading.Thread(name="CAS3", target=ca3.make_CAS, daemon=True).start()
        time.sleep(0.5)