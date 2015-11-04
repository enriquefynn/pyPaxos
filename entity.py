import gevent, socket, struct
from gevent import monkey
import utils
monkey.patch_socket()

class Entity(object):
    def __init__(self, name, config_path):
        config = utils.read_config(config_path)[name]
        self.__multicast_group = (config[0], int(config[1]))

        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 
            struct.pack('b', 1)) #Only live in the LAN
        self.__socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
            struct.pack('4sL', socket.inet_aton(self.__multicast_group[0]),
            socket.INADDR_ANY))
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__socket.bind(self.__multicast_group)
    
    def send(self, msg):
        self.__socket.sendto(msg.encode(), self.__multicast_group)

    def recv(self):
        print('Receiving...')
        msg, addr = self.__socket.recvfrom(1024) #TODO: Adjust buffer size?!
        print msg
        return (msg.decode(), addr)
