from __future__ import print_function
import logging

import grpc
import random
import time

from concurrent import futures
import rpcService_pb2
import rpcService_pb2_grpc

from multiprocessing import Process

def print_put_msg(response):
    if response.success:
        print("send PUT data to server success!!!!")
    else:
        if response.error_msg!=None:
            print("send PUT's error_msg: ", response.error_msg)

def print_get_msg(response, request):
    if response.success:
        print("GET data from server success!!!!")
        print("key: ", request['key'], "value: ", response.value)
    else:
        if response.error_msg!=None:
            print("GET's error_msg: ", response.error_msg)

def print_del_msg(response):
    if response.success:
        print("send DEL data in server success!!!!")
    else:
        if response.error_msg!=None:
            print("send DEL's error_msg: ", response.error_msg)

def print_client_operation(opera_type, data, port):
    if opera_type == 'put':
        print('PUT: ', data, 'to: ', 'localhost:'+str(port))
    elif opera_type == 'get':
        print('GET: ', data, 'from: ', 'localhost:'+str(port))
    else:
        print('DEL: ', data, 'to: ', 'localhost:'+str(port), 'and delete in all server.')

def print_server_response(response, data, opera_type):
    if opera_type == 'put':
        print_put_msg(response)
    if opera_type == 'get':
        print_get_msg(response, data)
    if opera_type == 'del':
        print_del_msg(response)



def PutDel(port, client_port, connect_timeout_inseconds, opera_type):
    with grpc.insecure_channel('localhost:'+str(port)) as channel:
        stub = rpcService_pb2_grpc.RPCStub(channel)

        # produce data for request
        if opera_type == 'put':
            value = str(time.time())
        else:
            value = None
        data = {'key': 'x', 'value': value, 'type':'client_append_entries', 'clientport':client_port, 'opera_type': opera_type}

        # print client operation
        print_client_operation(opera_type, data, port)
        # send request to server
        try:
            response = stub.PutDel(rpcService_pb2.putDelRequest(key=data['key'], value=data['value'], \
                type=data['type'], clientport=data['clientport'], opera_type=data['opera_type']), connect_timeout_inseconds)
            print_server_response(response, data, opera_type)
        except:
            print("connect server error!")
        print("\n")


def Get(port, client_port, connect_timeout_inseconds, opera_type):
    with grpc.insecure_channel('localhost:'+str(port)) as channel:
        stub = rpcService_pb2_grpc.RPCStub(channel)

        # produce data for request
        value = None
        data = {'key': 'x', 'value': value, 'type':'client_append_entries', 'clientport':client_port, 'opera_type': opera_type}

        # print client operation
        print_client_operation(opera_type, data, port)
        # send request to server
        try:
            response = stub.Get(rpcService_pb2.getRequest(key=data['key'], value=data['value'], \
                type=data['type'], clientport=data['clientport'], opera_type=data['opera_type']), connect_timeout_inseconds)
            print_server_response(response, data, opera_type)
        except:
            print("connect server error!")
        print("\n")



def send():
    servers_ports = [10001,10002,10003,10004]
    connect_timeout_inseconds = 0.1
    operations = ['get']#, 'get', 'del']
    client_port = str(10000)
    while True:
        port = random.choice(servers_ports)
        oper = random.choice(operations)
        if oper !='get':
            PutDel(port, client_port, connect_timeout_inseconds, oper)
        else:
            Get(port, client_port, connect_timeout_inseconds, oper)

        time.sleep(10)

# def recv():
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     rpcService_pb2_grpc.add_RPCServicer_to_server(clientSever(), server)
#     server.add_insecure_port('[::]:'+str(10000))
#     server.start()
#     server.wait_for_termination()


# class clientSever(rpcService_pb2_grpc.RPCServicer):
#     def Apply(self, request, context):
#         print("client recv: " + str(request.commit_index) + ' has been committed')
#         return rpcService_pb2.applyResponse(success=True)

if __name__ == '__main__':
    logging.basicConfig()

    send()
    # p1 = Process(target=send, name='send', daemon=True)
    # p1.start()
    # p2 = Process(target=recv, name='recv', daemon=True)
    # p2.start()


    # p1.join()
    # p2.join()
