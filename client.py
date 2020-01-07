from __future__ import print_function
import logging

import grpc
import random
import time

import rpcService_pb2
import rpcService_pb2_grpc

def run():
    servers_ports = [10001,10002,10003,10004]
    connect_timeout_inseconds = 0.1
    client_port = str(10000)
    while True:
        port = random.choice(servers_ports)

        with grpc.insecure_channel('localhost:'+str(port)) as channel:
            stub = rpcService_pb2_grpc.RPCStub(channel)
            data = {'key': 'x', 'value': str(time.time()), 'type':'client_append_entries', 'clientport':client_port}
            print('send: ', data, 'to: ', 'localhost:'+str(port))
            try:
                response = stub.Put(rpcService_pb2.putRequest(key=data['key'], value=data['value'], \
                    type=data['type'],clientport=data['clientport']), connect_timeout_inseconds)
                if response.success:
                    print("send data to server success!!!!")
            except:
                print("connect server error!")

        time.sleep(10)


class clientSever(rpcService_pb2_grpc.RPCServicer):
    def Apply(self, request, context):
        print("client recv: " + request.commit_index + ' has been committed')
        return rpcService_pb2.applyResponse(success=True)

if __name__ == '__main__':
    logging.basicConfig()
    run()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpcService_pb2_grpc.add_RPCServicer_to_server(clientSever(), server)
    server.add_insecure_port('[::]:'+client_port)
    server.start()
    server.wait_for_termination()