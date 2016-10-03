#!/usr/bin/env python
import argparse
import httplib
import re
import signal
import socket
import threading
import hashlib
import random
import time


from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from nameserver import node_addresses


# object_store = {}
node_list = []
node_addresses = []

class Node():
    initialized = 0
    n_id = 0
    address = ''
    object_store = {}
    successor = (0, '')
    predecessor = (0, '')
    pub_id = 0

    def insert_key(self, key, value):
        # key = int(key, 16)
        object_store[key] = value
        # print len(self.object_store)
        # if len(self.object_store) is 1:
        #     self.id = key
        print "inserted key: ", key, "and value: ", value, "on node: ", id
        print object_store

    def retreive_key(self, key):
        key = int(key, 16)
        print object_store[key]
        return object_store[key]

    def is_last(self):
        if node.next != 0:
            return False
        else:
            return True

    def find_successor(self, n_id):
        larger = 0
        smallest = 0
        largest = 0
        # print "inside find_successor, node_addresses: ", node_addresses
        # print "id = ", n_id
        for n in node_addresses:
            check_id = hash_value(n)
            if check_id > n_id and larger is 0:
                larger = check_id
                # largest = check_id
                # print "====larger====", larger

            if check_id > n_id and check_id < larger:
                # print '1, larger: ', larger, "checkid: ", check_id, "nid: ", n_id
                larger = check_id

            if check_id <= n_id and smallest is 0:
                # print '2, smallest: ', smallest, "checkid: ", check_id, "nid: ", n_id
                smallest = check_id

            if check_id <= n_id and check_id < smallest:
                # print "3, smallest: ", smallest, "checkid: ", check_id, "nid: ", n_id
                smallest = check_id

            if check_id >= n_id and largest is 0:
                # print '4, largest: ', largest, "checkid: ", check_id, "nid: ", n_id
                largest = check_id

            if check_id >= n_id and check_id > largest:
                # print '5, largest: ', largest, "checkid: ", check_id, "nid: ", n_id
                largest = check_id

        if largest == n_id:
            return smallest

        return larger

    def find_predecessor(self, n_id):
        smaller = 0
        smallest = 0
        largest = 0
        for n in node_addresses:
            check_id = hash_value(n)
            if check_id < n_id and smaller is 0:
                smaller = check_id
            if check_id < n_id and check_id > smaller:
                smaller = check_id

            if check_id <= n_id and smallest is 0:
                smallest = check_id
            if check_id <= n_id and check_id < smallest:
                smallest = check_id

            if check_id >= n_id and largest is 0:
                largest = check_id
            if check_id >= n_id and check_id > largest:
                largest = check_id

        if smallest == n_id:
            return largest

        return smaller


class NodeHttpHandler(BaseHTTPRequestHandler):
    # def __init__(self, node, *args):
        # self.node = node
        # self.args = args
        # print "args: ", args
        # BaseHTTPRequestHandler.__init__(self, args)

    def extract_key_from_path(self, path):
        return re.sub(r'/?(\w+)', r'\1', path)


    def do_SEND(self, node, key, value):
        conn = httplib.HTTPConnection(node)
        conn.request("PUT", "/"+key, value)
        conn.getresponse()
        conn.close

    def do_CONN(self):
        key = self.extract_key_from_path(self.path)
        h_key = hash_value(key)
        int_key = int(h_key)

        #If alone, first node in.
        if int_key == node.n_id:
            if node.successor[0] == 0:
                node.successor = (node.n_id, node.address)
            if node.predecessor[0] == 0:
                node.predecessor = (node.n_id, node.address)

        if int_key > node.n_id:
            if int_key > node.successor[0]:
                if node.successor[0] == node.n_id:
                    node.successor = (int_key, key)
                else:
                    connect_node(node.successor[1], key, 'CONN')
            elif int_key < node.successor[0]:
                tmp = (node.successor[0], node.successor[1])
                node.successor = (int_key, key)
                connect_node(tmp[1], key, 'CONN')
            else:
                return
        elif int_key < node.n_id:
            if int_key < node.predecessor[0]:
                if node.predecessor[0] == node.n_id:
                    node.predecessor = (int_key, key)
                else:
                    connect_node(node.predecessor[1], key, 'CONN')
            elif int_key > node.predecessor[0]:
                tmp = (node.predecessor[0], node.predecessor[1])
                node.predecessor = (int_key, key)
                connect_node(tmp[1], key, 'CONN')
            else:
                return

        self.send_response(200)
        self.end_headers()

    def do_PUT(self):
        content_length = int(self.headers.getheader('content-length', 0))

        key = self.extract_key_from_path(self.path)
        value = self.rfile.read(content_length)

        h_key = hash_value(key)
        int_key = int(h_key, 16)

        node.object_store[int_key] = value

        # Send OK response
        self.send_response(200)
        self.end_headers()


    def do_GET(self):
        key = self.extract_key_from_path(self.path)

        h_key = hash_value(key)
        int_key = int(h_key, 16)
        #node = node_list[0]

        for node in node_list:
            if int_key in node.object_store:
                self.send_response(200)
                self.send_header('Content-type','text/plain')
                self.send_header('Content-length',len(node.object_store[int_key]))
                self.end_headers()
                self.wfile.write(node.object_store[int_key])
                break
        #    else:
        #
        # self.send_response(404)
        # self.send_header('Content-type','text/plain')
        # self.end_headers()
        # self.wfile.write("No object with key '%s' on this node" % key)
        #

def initiate_new_node(id, next, prev):
    return Node(id, next, prev)

def hash_value(value):
    m = hashlib.sha1(value)
    m.hexdigest()
    # print m.hexdigest()
    short = m.hexdigest()
    # short = short[:6]
    short = int(short, 16)
    # print short
    return short

def parse_args():
    PORT_DEFAULT = 8000
    DIE_AFTER_SECONDS_DEFAULT = 20 * 60
    parser = argparse.ArgumentParser(prog="node", description="DHT Node")

    parser.add_argument("-p", "--port", type=int, default=PORT_DEFAULT,
            help="port number to listen on, default %d" % PORT_DEFAULT)

    parser.add_argument("--die-after-seconds", type=float,
            default=DIE_AFTER_SECONDS_DEFAULT,
            help="kill server after so many seconds have elapsed, " +
                "in case we forget or fail to kill it, " +
                "default %d (%d minutes)" % (DIE_AFTER_SECONDS_DEFAULT, DIE_AFTER_SECONDS_DEFAULT/60))

    parser.add_argument("--nameserver", type=str, required=False,
            help="address (host:port) of nameserver to register with")

    return parser.parse_args()


def connect_node(node, key, METHOD):
    conn = httplib.HTTPConnection(node)
    conn.request(METHOD, "/"+key)
    conn.getresponse()
    conn.close

def print_shit(node):
    print "node successor: ", node.successor, "node predecessor: ", node.predecessor, "node_id: ", node.n_id


def get_list_of_nodes(nameserver):
    conn = httplib.HTTPConnection(nameserver)
    conn.request("GET", "/", "")

    resp = conn.getresponse()
    addresses = resp.read()

    conn.close()

    return addresses.split()

def init_node(node, address):
    node.n_id = hash_value(address)
    node.address = address

if __name__ == "__main__":

    #node_nr = 0
    #number_of_nodes = 1
    node = Node()

    # node.set_id(0)
    # node_handler(node, 16)
    args = parse_args()
    # print args
    address = "%s:%d" % (socket.gethostname(), args.port)
    print address


    def node_api(node, *args):
        handler = NodeHttpHandler(node, args)

    # server = HTTPServer(('', args.port), node_api)
    server = HTTPServer(('', args.port), NodeHttpHandler)

    def run_server():
        print "Starting server on port" , args.port
        server.serve_forever()
        print "Server has shut down"

    def shutdown_server_on_signal(signum, frame):
        print "We get signal (%s). Asking server to shut down" % signum
        server.shutdown()

    # Start server in a new thread, because server HTTPServer.serve_forever()
    # and HTTPServer.shutdown() must be called from separate threads
    thread = threading.Thread(target=run_server)
    thread.daemon = True
    thread.start()

    # Shut down on kill (SIGTERM) and Ctrl-C (SIGINT)
    signal.signal(signal.SIGTERM, shutdown_server_on_signal)
    signal.signal(signal.SIGINT, shutdown_server_on_signal)

    # Register with nameserver
    if args.nameserver:
        my_address = "%s:%d" % (socket.gethostname(), args.port)
        conn = httplib.HTTPConnection(args.nameserver)
        conn.request("PUT", "/"+my_address, "")
        conn.getresponse()
        conn.close()
    # time.sleep(2)

    node_addresses = get_list_of_nodes(args.nameserver)
    print node_addresses

    init_node(node, address)
    rand_node = random.choice(node_addresses)
    connect_node(rand_node, node.address, 'CONN')
    print_shit(node)


    # print node_addresses
    # Wait on server thread, until timeout has elapsed
    #
    # Note: The timeout parameter here is also important for catching OS
    # signals, so do not remove it.
    #
    # Having a timeout to check for keeps the waiting thread active enough to
    # check for signals too. Without it, the waiting thread will block so
    # completely that it won't respond to Ctrl-C or SIGTERM. You'll only be
    # able to kill it with kill -9.
    thread.join(args.die_after_seconds)
    if thread.isAlive():
        print "Reached %.3f second timeout. Asking server to shut down" % args.die_after_seconds
        server.shutdown()

    print "Exited cleanly"
