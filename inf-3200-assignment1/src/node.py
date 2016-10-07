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

node_addresses = []

class Node():
    def __init__(self, address):
        self.address = address
        self.id = hash_value(self.address)
        self.object_store = {}
        self.successor = (0, '')
        self.predecessor = (0, '')

    def find_successor(self):
        larger = (0, '')
        smallest = (0, '')
        largest = (0, '')
        global node_addresses
        
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

    def node_put(self, key, value):
        h_key = hash_value(key)
        h_key = int(h_key)

        if (h_key > self.id or h_key < self.successor[0]):
            return h_key
        elif (h_key > self.id and h_key > self.successor[0] and self.successor[0] < node.id):
            return h_key
        else:
            conn = httplib.HTTPConnection(self.successor[1])
            conn.request('PUT', '/'+key, value)
            conn.getresponse()
            conn.close()
            return 0

    def node_get(self, key):
        conn = httplib.HTTPConnection(self.successor[1])
        conn.request('GET', '/'+key)
        resp = conn.getresponse()
        conn.close()
        retval = resp.read()
        return retval


class NodeHttpHandler(BaseHTTPRequestHandler):
    def extract_key_from_path(self, path):
        return re.sub(r'/?(\w+)', r'\1', path)

    def do_PUT(self):
        content_length = int(self.headers.getheader('content-length', 0))

        print "NodeID: ", node.id
        key = self.extract_key_from_path(self.path)
        value = self.rfile.read(content_length)

        if key == 'update' and value == '-1':
            global node_addresses
            node_addresses = get_list_of_nodes(args.nameserver)
            init_node(node)
            self.send_response(200)
            self.end_headers()
            return

        h_key = hash_value(key)
        int_key = int(h_key)

        new_key = node.node_put(key, value)

        if new_key != 0:
            node.object_store[new_key] = value
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(200)
            self.end_headers()

        # if (int_key > node.id and int_key < node.successor[0]):
            # node.object_store[int_key] = value
        # elif (int_key < node.id):
            # conn = httplib.HTTPConnection(node.predecessor[1])
            # conn.request('PUT', '/'+key, value)
            # conn.getresponse()
            # conn.close()
        # elif (int_key > node.id and int_key > node.successor[0]):
            # conn = httplib.HTTPConnection(node.successor[1])
            # conn.request('PUT', '/'+key, value)
            # conn.getresponse()
            # conn.close()
        # elif (int_key > node.id and int_key > node.successor[0] and node.successor[0] < node.id):
            # node.object_store[int_key] = value
        # else:
            # self.send_response(404)
            # self.end_headers()

        # Send OK response
        # self.send_response(200)
        # self.end_headers()


    def do_GET(self):
        key = self.extract_key_from_path(self.path)

        h_key = hash_value(key)
        int_key = int(h_key)

        if int_key in node.object_store:
            self.send_response(200)
            self.send_header('Content-type','text/plain')
            self.send_header('Content-length',len(node.object_store[int_key]))
            self.end_headers()
            self.wfile.write(node.object_store[int_key])
        else:
            value = node.node_get(key)
            print type(value), value
            
            if value == 0:
                self.send_response(404)
                self.send_header('Content-type','text/plain')
                self.end_headers()
                self.wfile.write("No object with key '%s' on this node" % key)

            self.send_response(200)
            self.send_header('Content-type','text/plain')
            self.send_header('Content-length', len(value))
            self.end_headers()
            self.wfile.write(value)

        
        # elif (int_key < node.id):
            # conn = httplib.HTTPConnection(node.predecessor[1])
            # conn.request('GET', '/'+key)
            # conn.getresponse()
            # conn.close()
        # elif (int_key > node.id and int_key > node.successor[0]):
            # conn = httplib.HTTPConnection(node.successor[1])
            # conn.request('GET', '/'+key)
            # conn.getresponse()
            # conn.close
        # else:
            # self.send_response(404)
            # self.send_header('Content-type','text/plain')
            # self.end_headers()
            # self.wfile.write("No object with key '%s' on this node" % key)

def hash_value(value):
    m = hashlib.sha1(value)
    m.hexdigest()
    short = m.hexdigest()
    short = int(short, 16)
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

if __name__ == "__main__":
    args = parse_args()
    address = "%s:%d" % (socket.gethostname(), args.port)

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

    node = Node(address)

    global node_addresses
    node_addresses = get_list_of_nodes(args.nameserver)

    init_node(node)

    for node_addr in node_addresses:
        conn = httplib.HTTPConnection(node_addr)
        conn.request('PUT', '/update', '-1')
        conn.getresponse()
        conn.close()

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
