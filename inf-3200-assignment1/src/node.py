#!/usr/bin/env python
import argparse
import httplib
import re
import signal
import socket
import threading
import hashlib

from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer


object_store = {}
node_list = []

class Node():
    def __init__(self):
        self.id = 0
        self.object_store = {}
        self.next = 0
        self.prev = 0
        self.pub_id = 0

    def insert_key(self, key, value):
        # if self.check_storage():
        self.object_store[key] = value
        if len(object_store) is 1:
            self.pub_id = object_store.itervalues().next()
        print "inserted key: ", key, "and value: ", value
        print self.object_store

    def is_last(self):
        if node.next != 0:
            return False
        else:
            return True

    def set_id(self, id):
        self.id = id

    #
    # def check_storage(self):
    #     if len(self.object_store) >= 1:
    #         return False
    #     else:
    #         return True



class NodeHttpHandler(BaseHTTPRequestHandler):

    def extract_key_from_path(self, path):
        return re.sub(r'/?(\w+)', r'\1', path)

    def do_PUT(self):
        content_length = int(self.headers.getheader('content-length', 0))

        key = self.extract_key_from_path(self.path)
        value = self.rfile.read(content_length)

        h_key = hash_value(value)

        for node in node_list:
            if (h_key > node.pub_id and h_key not in node.object_store):
                node.insert_key(h_key, value)
            elif node.is_last():
                node.insert_key(h_key, value)

        #
        # if (len(node_list) is 0):
        #     node = initiate_new_node(0, 0, 0)
        #     print "made initial node"
        #     node_list.append(node)
        #     print node_list
        # else:
        #     node = node_list[-1]
        #
        #
        #
        # if (node.check_storage()):
        #     node.insert_key(key, value)
        # else:
        #     node = initiate_new_node(len(node_list), node, 0)
        #     print "made a new node"
        #     node_list.append(node)
        #     print node_list
        #     node.insert_key(key, value)

        #object_store[key] = value

        # Send OK response
        self.send_response(200)
        self.end_headers()


    def do_GET(self):
        key = self.extract_key_from_path(self.path)

        #node = node_list[0]

        for node in node_list:
            if key in node.object_store:
                self.send_response(200)
                self.send_header('Content-type','text/plain')
                self.send_header('Content-length',len(node.object_store[key]))
                self.end_headers()
                self.wfile.write(node.object_store[key])
                break
        #    else:

        self.send_response(404)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        self.wfile.write("No object with key '%s' on this node" % key)


        # if key in node.object_store:
        #     self.send_response(200)
        #     self.send_header('Content-type','text/plain')
        #     self.send_header('Content-length',len(node.object_store[key]))
        #     self.end_headers()
        #     self.wfile.write(node.object_store[key])
        # else:
        #     self.send_response(404)
        #     self.send_header('Content-type','text/plain')
        #     self.end_headers()
        #     self.wfile.write("No object with key '%s' on this node" % key)


def initiate_new_node(id, next, prev):
    return Node(id, next, prev)

def hash_value(value):
    m = hashlib.sha1()
    m.update(value)
    m.hexdigest()
    print m
    print m.digest_size
    return m

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

def node_handler(first_node, nr_nodes):
    node_counter = first_node
    for i in range(nr_nodes - 1):
        node = Node()
        node.id = i + 1
        node.prev = node_counter
        node_counter.next = node
        node_counter = node
        node_list.append(node)




if __name__ == "__main__":

    #node_nr = 0
    #number_of_nodes = 1
    node = Node()
    node.set_id(0)
    node_handler(node, 16)
    args = parse_args()




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
