# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

from node import Node

from concurrent import futures
import grpc

import rpcService_pb2
import rpcService_pb2_grpc

if __name__ == '__main__':

    conf = {'id': 'node_2',
              'addr': ('localhost', 10002),
              'peers': { 'node_1': ('localhost', 10001),
                         'node_3': ('localhost', 10003),
                         'node_4': ('localhost', 10004)
                       }
            }
     
    node = Node(conf)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpcService_pb2_grpc.add_RPCServicer_to_server(node, server)
    server.add_insecure_port('[::]:'+str(10002))
    server.start()
    server.wait_for_termination()