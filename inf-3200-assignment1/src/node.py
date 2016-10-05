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
    def __init__(self, address):
        self.address = address
        self.id = hash_value(self.address)
        self.object_store = {}
        self.successor = (0, '')
        self.predecessor = (0, '')
        # neighbour = str(successor[1] + predecessor[1])

    def insert_key(self, key, value):
        # key = int(key, 16)
        object_store[key] = value
        # print len(self.object_store)
        # if len(self.object_store) is 1:
        #     self.id = key
        # print "inserted key: ", key, "and value: ", value, "on node: ", id
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

    def find_successor(self):
        larger = (0, '')
        smallest = (0, '')
        largest = (0, '')
        global node_addresses
        # print "inside find succ list: ", node_addresses
        for node in node_addresses:
            compare_id = hash_value(node)

            if compare_id > self.id and larger[0] is 0:
                larger = (compare_id, node)
            if compare_id > self.id and compare_id < larger[0]:
                larger = (compare_id, node)
            if compare_id <= self.id and smallest[0] is 0:
                smallest = (compare_id, node)
            if compare_id <= self.id and compare_id < smallest[0]:
                smallest = (compare_id, node)
            if compare_id >= self.id and largest[0] is 0:
                largest = (compare_id, node)
            if compare_id >= self.id and compare_id > largest[0]:
                largest = (compare_id, node)

        if largest[0] == self.id:
            return smallest

        return larger

    def find_predecessor(self):
        smaller = (0, '')
        smallest = (0, '')
        largest = (0, '')
        global node_addresses
        for node in node_addresses:
            compare_id = hash_value(node)

            if compare_id < self.id and smaller[0] is 0:
                smaller = (compare_id, node)
            if compare_id < self.id and compare_id > smaller[0]:
                smaller = (compare_id, node)
            if compare_id <= self.id and smallest[0] is 0:
                smallest = (compare_id, node)
            if compare_id <= self.id and compare_id < smallest[0]:
                smallest = (compare_id, node)
            if compare_id >= self.id and largest[0] is 0:
                largest = (compare_id, node)
            if compare_id >= self.id and compare_id > largest[0]:
                largest = (compare_id, node)

        if smallest[0] == self.id:
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

    # def do_POST(self):
    #     key = self.extract_key_from_path(self.path)
    #     h_key = hash_value(key)
    #     int_key = int(h_key)
    #     tmp = (0, '')
    #
    #     print "key", key
    #
    #     #If alone, first node in.
    #     if int_key == node.id:
    #         if node.successor[0] == 0:
    #             node.successor = (node.id, node.address)
    #         if node.predecessor[0] == 0:
    #             node.predecessor = (node.id, node.address)
    #         print "FIRST BITCHES"
    #         node.neighbour = str((str(node.successor[1]), str(node.predecessor[1])))
    #
    #
    #     if int_key > node.id:
    #         if int_key > node.successor[0]:
    #             if node.successor[0] == node.id:
    #                 node.successor = (int_key, key)
    #                 print node.successor
    #                 connect_node(key, node.address, 'POST')
    #                 self.send_response(200)
    #                 self.end_headers()
    #             else:
    #                 print "[1] Connect to node: ", node.successor[1], "(successor) with key ", key
    #                 # self.send_response(200)
    #                 # self.end_headers()
    #                 # connect_node(key, node.successor[1], 'POST')
    #         elif int_key < node.successor[0]:
    #             tmp = (node.successor[0], node.successor[1])
    #             node.successor = (int_key, key)
    #             print "[2] Connect to node: ", node.successor[1], " (successor) with key ", key
    #             # self.send_response(200)
    #             # self.end_headers()
    #             # connect_node(key, tmp[1], 'POST')
    #         else:
    #             return
    #     elif int_key < node.id:
    #         if int_key < node.predecessor[0]:
    #             if node.predecessor[0] == node.id:
    #                 node.predecessor = (int_key, key)
    #             else:
    #                 print "[3] Connect to node: ", node.predcessor[1], "(predecessor) with key ", key
    #                 # self.send_response(200)
    #                 # self.end_headers()
    #                 # connect_node(key, node.predecessor[1], 'POST')
    #         elif int_key > node.predecessor[0]:
    #             tmp = (node.predecessor[0], node.predecessor[1])
    #             node.predecessor = (int_key, key)
    #             print "[4] Connect to node with tmp: ", tmp[1], "with key ", key
    #             # self.send_response(200)
    #             # self.end_headers()
    #             # connect_node(key, tmp[1], 'POST')
    #         else:
    #             return
    #
    #     elif node.successor[0] == 0:
    #         if tmp[0] != 0: # is the next
    #             node.successor = (tmp[0], temp[1])
    #
    #     print_shit(node)
    #
    #     print "node.neighbour: ", node.neighbour
    #     self.send_response(200)
    #     self.end_headers()

    def do_PUT(self):
        content_length = int(self.headers.getheader('content-length', 0))

        key = self.extract_key_from_path(self.path)
        value = self.rfile.read(content_length)

        if key == 'update' and value == '-1':
            global node_addresses
            node_addresses = get_list_of_nodes(args.nameserver)
            init_node(node)
            # print "inside extra update"
            # print "node list:", node_addresses
            # print_shit(node)
            self.send_response(200)
            self.end_headers()
            return

        h_key = hash_value(key)
        int_key = int(h_key)

        node.object_store[int_key] = value

        # Send OK response
        self.send_response(200)
        self.end_headers()


    def do_GET(self):
        key = self.extract_key_from_path(self.path)

        h_key = hash_value(key)
        int_key = int(h_key)
        #node = node_list[0]

        if int_key in node.object_store:
            self.send_response(200)
            self.send_header('Content-type','text/plain')
            self.send_header('Content-length',len(node.object_store[int_key]))
            self.end_headers()
            self.wfile.write(node.object_store[int_key])
        else:
            self.send_response(404)
            self.send_header('Content-type','text/plain')
            self.end_headers()
            self.wfile.write("No object with key '%s' on this node" % key)


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
    resp = conn.getresponse()
    conn.close
    data = resp.read()
    print "received data: ", data

def print_shit(node):
    print "node successor: ", node.successor, "node predecessor: ", node.predecessor, "node_id: ", node.id


def get_list_of_nodes(nameserver):
    conn = httplib.HTTPConnection(nameserver)
    conn.request("GET", "/", "")

    resp = conn.getresponse()
    addresses = resp.read()

    conn.close()

    return addresses.split()

def init_node(node):
    node.successor = node.find_successor()
    node.predecessor = node.find_predecessor()
    # print_shit(node)


if __name__ == "__main__":

    #node_nr = 0
    #number_of_nodes = 1

    # node.set_id(0)
    # node_handler(node, 16)
    args = parse_args()
    # print args
    address = "%s:%d" % (socket.gethostname(), args.port)
    # print address

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

    node = Node(address)

    global node_addresses
    node_addresses = get_list_of_nodes(args.nameserver)
    # print node_addresses

    # print "init node first time"
    init_node(node)


    for node_addr in node_addresses:
        conn = httplib.HTTPConnection(node_addr)
        conn.request('PUT', '/update', '-1')
        conn.getresponse()
        conn.close()

    # rand_node = random.choice(node_addresses)
    # rand_node = node_addresses[0]
    # print_shit(node)
    # connect_node(rand_node, node.address, 'POST')


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
