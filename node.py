from concurrent import futures
import logging

import grpc
from grpc._channel import _Rendezvous

import rpcService_pb2
import rpcService_pb2_grpc

import os
import json
import random

from log import Log
from threading import Timer
from utils import *
from constant import *

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Node(rpcService_pb2_grpc.RPCServicer):
    def __init__(self, conf):
        self.role = 'follower'
        self.id = conf['id']
        self.addr = conf['addr']

        self.peers = conf['peers']

        # persistent state
        self.current_term = 0
        self.voted_for = None
        self.kv = {}

        if not os.path.exists(self.id):
            os.mkdir(self.id)

        # init persistent state
        self.load()
        self.log = Log(self.id)

        # volatile state
        # rule 1, 2
        self.commit_index = 0
        self.last_applied = 0

        # volatile state on leaders
        # rule 1, 2
        self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers} #log_list最后一个index的下一个，也就是还是空的 #{16+1，16+1，16+1, ...}
        self.match_index = {_id: -1 for _id in self.peers}

        # append entries
        self.leader_id = None

        # request vote
        self.vote_ids = {_id: 0 for _id in self.peers}

        # client request
        self.client_port = None

        # tick
        self.t_heartbeat = 2
        self.next_leader_election = 3
        self.connect_timeout_in_seconds=0.1
        self.wait_s = (1, 2)
        ## 心跳或者entry msg计时器/或者叫选举计时器
        self.next_leader_election_timer_restart()

        self.cmd_output = {'msgType':'info',
                            'msg':'become follower',
                            'localID':self.id,
                            'currentTerm':self.current_term}
        print(str(self.cmd_output))

    # tick func
    def next_leader_election_timer_restart(self):
        self.next_leader_election_timer = Timer(self.next_leader_election + random.uniform(*self.wait_s), self.election_timeoutStep)
        self.next_leader_election_timer.start()
    def next_leader_election_timer_cancel(self):
        self.next_leader_election_timer.cancel()
    def next_heartbeat_timer_restart(self):
        self.next_heartbeat_timer = Timer(self.t_heartbeat + random.uniform(*self.wait_s), self.next_heartbeat_timeoutStep)
        self.next_heartbeat_timer.start()
    def next_heartbeat_timer_cancel(self):
        self.next_heartbeat_timer.cancel()


    def load(self):
        file_path = self.id + '/key.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)

            self.current_term = data['current_term']
            self.voted_for = data['voted_for']
            self.kv = data['kv']
        else:
            self.save()

    def save(self):
        data = {'current_term': self.current_term,
                'voted_for': self.voted_for,
                'kv': self.kv,
                }

        file_path = self.id + '/key.json'
        with open(file_path, 'w') as f:
            json.dump(data, f)

    def election_timeoutStep(self):
        '''
        开始选举前的timeout处理（长时间未收到消息）， 只有follower会触发
        选举中的timeout处理， 只有candidate会触发
        '''
        # logging.info('ENTER election_timeoutStep')
        if self.role == 'follower':
            self.follower_do(task='becomeCandidate')
            # print("\n")
            return
        if self.role == 'candidate':
            self.candidate_do(task='reElection')
            # print("\n")
            return

    def next_heartbeat_timeoutStep(self):
        '''
        只有leader会触发
        '''
        # logging.info('ENTER next_heartbeat_timeoutStep')
        if self.role == 'leader':
            self.next_heartbeat_timer_cancel()
            for dst_id in self.peers:
                # logging.info('leader：1. send append_entries to peer ' + dst_id)
                peer_addr = str(self.peers[dst_id][0]) + ':' + str(self.peers[dst_id][1])

                with grpc.insecure_channel(peer_addr) as channel:
                        stub = rpcService_pb2_grpc.RPCStub(channel)
                        response = None
                        try:
                            response = stub.AppendEntries(rpcService_pb2.appendEntriesRequest(term=self.current_term,
                                                                                        leaderId=self.id,
                                                                                        prev_log_index=self.next_index[dst_id] - 1,
                                                                                        prev_log_term=self.log.get_log_term(self.next_index[dst_id] - 1),
                                                                                        entries=self.log.get_entries(self.next_index[dst_id]), #(16+1，就是我这个leader的log里面的最后一个的下一个|第0个)
                                                                                        leader_commit=self.commit_index), self.connect_timeout_in_seconds)
                        except:
                            # print("----------------------------send appendrpc connect error!")
                            self.cmd_output = {}
                            self.cmd_output['msgType']='info'
                            self.cmd_output['msg']='send msg fail'
                            self.cmd_output['localID']=self.id
                            self.cmd_output['to']=dst_id
                            self.cmd_output['send_msg_type']='MsgHeartBeat' if self.log.get_entries(self.next_index[dst_id]) == [] else 'MsgApp'
                            print(str(self.cmd_output))

                        if response !=None and response.type!='heartbeat_response':
                            self.leader_do(data=response)
            # 发送完心跳包，重新开始计时
            self.next_heartbeat_timer_restart()
            # print("\n")

    def AppendEntries(self, request, context):
        '''
        # 不为空，也不是从client发来的信息
        # 根据是headbeart还是append，决定要做什么事情
        '''
        self.all_do()

        self.next_leader_election_timer_cancel()
        self.next_leader_election_timer_restart()

        self.leader_id = request.leaderId
        # logging.info(self.role + '1. recv append_entries from leader ' + self.leader_id)
        self.cmd_output={}
        self.cmd_output={
            'msgType':'MsgApp_or_MsgHeartBeat',
            'msg':'received from leader',
            'localID':self.id,
            'leaderID':self.leader_id,
            'currentTerm':self.current_term,
            'msgTerm':request.term
        }
        if request.entries != []:
            self.cmd_output['msgType']='MsgApp'#不为空，也可能是心跳包，所以定个标准，为空就是心跳包
            print(str(self.cmd_output))

        if request.term < self.current_term:
            # logging.info(self.role + ': 2. smaller term')
            # logging.info(self.role + ': 3. success = False: smaller term')
            # logging.info(self.role + ': 4. send append_entries_response to leader ' + self.leader_id)
            # print("\n")
            return rpcService_pb2.appendEntriesResponse(responserId=self.id,
            success=False, responserTerm=self.current_term,
            type='append_entries_response')
        elif request.term > self.current_term: #针对candidate收到leader的消息的情况(别人先竞选成功了)
            # logging.info(self.role + ': 2. bigger term')
            # logging.info(self.role + ': 3. become follower')
            self.role = 'follower'
            self.current_term = request.term
            self.voted_for = None
            self.save()

        if self.role == 'candidate': #不管是不是心跳包，收到了就得转为follower
            self.candidate_do(task='convertToFollower')

        prev_log_index = request.prev_log_index
        prev_log_term = request.prev_log_term
        tmp_prev_log_term = self.log.get_log_term(prev_log_index)

        # self.cmd_output={}
        # self.cmd_output={
        #     'msgType':'info',
        #     'msg':'call maybeAppend',
        #     'localID':self.id,
        #     'leaderID':self.leader_id,
        #     'prevLogIndex':request.prev_log_index,
        #     'prevLogTerm':request.prev_log_term,
        #     'leaderCommitted':request.leader_commit
        # }
        # print(str(self.cmd_output))

        # append_entries: rule 2, 3
        # append_entries: rule 3
        if tmp_prev_log_term != prev_log_term:
            # logging.info(self.role + ': 5. success = False: index not match or term not match')
            # logging.info(self.role + ': 6. send append_entries_response to leader ' + self.leader_id)
            # logging.info(self.role + ': 7. log delete_entries')
            # logging.info(self.role + ': 8. log save')
            self.cmd_output={}
            self.cmd_output={
                'msgType':'info',
                'msg':'handleAppendEntries append failed and need to be consistent',
                'localID':self.id,
                'leaderID':self.leader_id,
                'prevLogIndex':request.prev_log_index,
                'prevLogTerm':request.prev_log_term,
                'leaderCommitted':request.leader_commit,
                'last_applied':self.last_applied,
                'lastLogIndex':self.log.last_log_index,
                'lastLogTerm':self.log.last_log_term
            }
            print(str(self.cmd_output))

            self.log.delete_entries(prev_log_index)
            # print("\n")
            return rpcService_pb2.appendEntriesResponse(responserId=self.id,
            success=False, responserTerm=self.current_term,
            type='append_entries_response')

        else:
            # logging.info(self.role + ': 5. success = True')
            # logging.info(self.role + ': 6. send append_entries_response to leader ' + self.leader_id)

            # 先把当前follower从prev_log_index开始的后面的部分（不包括prev_log_index）去掉，也就是强制覆盖的原则，比leader多出的log要全部去掉
            self.log.delete_entries(prev_log_index+1)

            if request.entries != []: #不为心跳
                # logging.info(self.role + ': 7. append_entries not None')
                # logging.info(self.role + ': 8. log append_entries')
                # logging.info(self.role + ': 9. log save')
                self.log.append_entries(prev_log_index, list(request.entries))
                self.cmd_output={}
                self.cmd_output={
                    'msgType':'info',
                    'msg':'handleAppendEntries append success',
                    'localID':self.id,
                    'leaderID':self.leader_id,
                    'prevLogIndex':request.prev_log_index,
                    'prevLogTerm':request.prev_log_term,
                    'leaderCommitted':request.leader_commit,
                    'last_applied':self.last_applied,
                    'lastLogIndex':self.log.last_log_index,
                    'lastLogTerm':self.log.last_log_term
                }
                print(str(self.cmd_output))

            # append_entries rule 5
            leader_commit = request.leader_commit
            if leader_commit > self.commit_index:
                commit_index = min(leader_commit, self.log.last_log_index)
                self.commit_index = commit_index+1###debug
                # logging.info(self.role + ': 7. commit_index = ' + str(commit_index))

            self.all_do()
            # print("\n")
            return rpcService_pb2.appendEntriesResponse(responserId=self.id,
            success=True, responserTerm=self.current_term,
            type='append_entries_response')


    def RequestVote(self, request, context):
        '''
        #不为空，也不是从client发来的信息
        '''
        self.next_leader_election_timer_cancel()
        self.next_leader_election_timer_restart()
        self.cmd_output = {}
        self.cmd_output = {
            'msgType':'MsgVote',
            'msg':'received MsgVote',
            'localID':self.id,
            'msgFrom':request.candidateId,
            'msgTerm':request.term,
            'msgLastLogTerm':request.last_log_term,
            'msgLastLogIndex':request.last_log_index,
            'LastLogTerm':self.log.last_log_term,
            'LastLogIndex':self.log.last_log_index
        }
        # logging.info(self.role + ': 1. recv request_vote from candidate ' + request.candidateId)

        if request.term < self.current_term:
            # logging.info(self.role + ': 2. smaller term, request.term: '+ str(request.term))
            # logging.info(self.role + ': 3. success = False')
            # logging.info(self.role + ': 4. send request_vote_response to candidate ' + request.candidateId)
            # logging.info(self.role + ': 5. current term: ' + str(self.current_term))
            # print(self.role, ': 6. voted for: ', self.voted_for)
            self.cmd_output['msg']='received MsgVote and vote reject'
            self.cmd_output['vote_for']=self.voted_for
            self.cmd_output['current_term']=self.current_term
            print(str(self.cmd_output))
            # print("\n")
            return rpcService_pb2.requsetVoteResponse(responserId=self.id, responserTerm=self.current_term, votedGranted=False)
        elif request.term > self.current_term: #针对follower收到candidate的消息的情况
            # logging.info(self.role + ': 2. bigger term')
            # logging.info(self.role + ': 3. become follower')
            self.role = 'follower'
            self.current_term = request.term
            self.voted_for = None
            self.save()

        candidate_id = request.candidateId
        last_log_index = request.last_log_index
        last_log_term = request.last_log_term

        # self.current_term = request.term
        if self.voted_for == None or self.voted_for == candidate_id:
            if self.role == 'leader' and request.last_log_index == self.log.last_log_index and request.last_log_term == self.log.last_log_term:
                # logging.info(self.role + ': 4. same log and ' + self.id + ' is leader already.')
                # logging.info(self.role + ': 5. send request_vote_response to candidate ' + request.candidateId)
                # logging.info(self.role + ': 6. current term: ' + str(self.current_term))
                # print(self.role, ': 7. voted for: ', self.voted_for)
                self.cmd_output['msg']='received MsgVote and vote reject'
                self.cmd_output['vote_for']=self.voted_for
                self.cmd_output['current_term']=self.current_term
                print(str(self.cmd_output))
                # print("\n")
                return rpcService_pb2.requsetVoteResponse(responserId=self.id, responserTerm=self.current_term, votedGranted=False)
            elif request.last_log_index >= self.log.last_log_index and request.last_log_term >= self.log.last_log_term:
                self.voted_for = request.candidateId
                self.save()
                # logging.info(self.role + ': 4. success = True: candidate log is newer')
                # logging.info(self.role + ': 5. send request_vote_response to candidate ' + request.candidateId)
                # logging.info(self.role + ': 6. current term: ' + str(self.current_term))
                # print(self.role, ': 7. voted for: ', self.voted_for)
                self.cmd_output['msg']='received MsgVote and vote granted'
                self.cmd_output['vote_for']=self.voted_for
                self.cmd_output['current_term']=self.current_term
                print(str(self.cmd_output))
                # print("\n")
                return rpcService_pb2.requsetVoteResponse(responserId=self.id, responserTerm=self.current_term, votedGranted=True)
            else:
                self.voted_for = None
                self.save()
                # logging.info(self.role + ': 4. success = False: candidate log is older')
                # logging.info(self.role + ': 5. send request_vote_response to candidate ' + request.candidateId)
                # logging.info(self.role + ': 6. current term: ' + str(self.current_term))
                # print(self.role, ': 7. voted for: ', self.voted_for)
                self.cmd_output['msg']='received MsgVote and vote reject'
                self.cmd_output['vote_for']=self.voted_for
                self.cmd_output['current_term']=self.current_term
                print(str(self.cmd_output))
                # print("\n")
                return rpcService_pb2.requsetVoteResponse(responserId=self.id, responserTerm=self.current_term, votedGranted=False)
        else:
            # logging.info(self.role + ': 4. success = False: has voted for ' + self.voted_for)
            # logging.info(self.role + ': 5. send request_vote_response to candidate ' + request.candidateId)
            # logging.info(self.role + ': 6. current term: ' + str(self.current_term))
            # print(self.role, ': 7. voted for: ', self.voted_for)
            self.cmd_output['msg']='received MsgVote and vote reject'
            self.cmd_output['vote_for']=self.voted_for
            self.cmd_output['current_term']=self.current_term
            print(str(self.cmd_output))
            # print("\n")
            return rpcService_pb2.requsetVoteResponse(responserId=self.id, responserTerm=self.current_term, votedGranted=False)

    def Get(self, request, context):
        # logging.info('ENTER Get')
        # TODO: 如果是leader，更新自己的entries，通知peers，大部分回复后执行并返回结果
        # TODO: 如果不是leader，则转发给leader
        self.client_port = request.clientport
        # logging.info(self.role + ': ' + 'recv GET from client ' + self.client_port + '!!!!!!!!!!')
        if self.role =='follower' and self.leader_id:
            if request.opera_type == 'get':
                self.cmd_output = {}
                self.cmd_output['msgType']='get'
                self.cmd_output['msg']='received GET command from client'
                self.cmd_output['localID']=self.id
                print(str(self.cmd_output))
                # logging.info(self.role + ': ' + 'Getredirect: GET client_append_entries to leader ' + self.leader_id)
                with grpc.insecure_channel('localhost:'+str(self.peers[self.leader_id][1])) as channel:
                    stub = rpcService_pb2_grpc.RPCStub(channel)
                    try:
                        getRedirect_response = stub.GetRedirect(rpcService_pb2.getRedirectRequest(key=request.key, \
                            value=request.value, type=request.type, clientport=request.clientport, opera_type=request.opera_type), self.connect_timeout_in_seconds)
                        # print("\n")
                        self.cmd_output = {}
                        self.cmd_output['msgType']='info'
                        self.cmd_output['msg']='redirect clientRequest to leader success'
                        self.cmd_output['localID']=self.id
                        self.cmd_output['leaderID']=self.leader_id
                        print(str(self.cmd_output))
                        if getRedirect_response.success:
                            return rpcService_pb2.getResponse(success=True, error_msg=None, value=getRedirect_response.value)
                        else:
                            return rpcService_pb2.getResponse(success=False, error_msg=getRedirect_response.error_msg, value=None)
                    except:
                        self.cmd_output = {}
                        self.cmd_output['msgType']='info'
                        self.cmd_output['msg']=' redirect clientRequest to leader failed, maybe connect error'
                        self.cmd_output['localID']=self.id
                        self.cmd_output['leaderID']=self.leader_id
                        print(str(self.cmd_output))
                        # print("----------------------------getRedirect connect error!")
                        # print("\n")
                        return rpcService_pb2.getResponse(success=False, error_msg='getRedirect connect error', value=None)
            elif request.opera_type == 'get_noredirect':
                self.cmd_output = {}
                self.cmd_output['msgType']='get_noredirect'
                self.cmd_output['msg']='received GET command from client'
                self.cmd_output['localID']=self.id
                print(str(self.cmd_output))
                if request.key not in self.kv:
                    return rpcService_pb2.getResponse(success=False, error_msg="GET Defeated!! Key not in leader state machine!!!", value=None)
                return rpcService_pb2.getResponse(success=True, error_msg=None, value=self.kv[request.key])

        elif self.role == 'candidate':
            # logging.info(self.role + ': ' + 'no leader!!!')
            # print("\n")
            if request.opera_type=='get':
                self.cmd_output = {}
                self.cmd_output['msgType']='get'
                self.cmd_output['msg']="received GET command from client, however, there is no leader"
                self.cmd_output['localID']=self.id
                print(str(self.cmd_output))
                return rpcService_pb2.getResponse(success=False, error_msg='no leader!!!', value=None)
            elif request.opera_type == 'get_noredirect':
                self.cmd_output = {}
                self.cmd_output['msgType']='get_noredirect'
                self.cmd_output['msg']='received GET command from client'
                self.cmd_output['localID']=self.id
                print(str(self.cmd_output))
                if request.key not in self.kv:
                    return rpcService_pb2.getResponse(success=False, error_msg="GET Defeated!! Key not in leader state machine!!!", value=None)
                return rpcService_pb2.getResponse(success=True, error_msg=None, value=self.kv[request.key])
        else:
            key = request.key
            self.cmd_output = {}
            self.cmd_output['msgType']='get'
            self.cmd_output['msg']='received GET command from client'
            self.cmd_output['localID']=self.id
            print(str(self.cmd_output))

            if key not in self.kv:
                # print("leader: GET Defeated!! Key not in leader state machine!!!")
                # print("\n")
                return rpcService_pb2.getResponse(success=False, error_msg="GET Defeated!! Key not in leader state machine!!!", value=None)
            # print("\n")
            return rpcService_pb2.getResponse(success=True, error_msg=None, value=self.kv[key])

    def GetRedirect(self, request, context):
        # logging.info('ENTER GetRedirect')
        self.client_port = request.clientport
        self.cmd_output = {}
        self.cmd_output['msgType']='info'
        self.cmd_output['msg']="received GetRedirect command from follower's redirect"
        self.cmd_output['localID']=self.id
        print(str(self.cmd_output))

        key = request.key
        if key not in self.kv:
            # print("leader: GET Defeated!! Key not in leader state machine!!!")
            # print("\n")
            return rpcService_pb2.getRedirectResponse(success=False, error_msg="GET Defeated!! Key not in leader state machine!!!", value=None)
        # print("\n")
        return rpcService_pb2.getRedirectResponse(success=True, error_msg=None, value=self.kv[key])

    def PutDel(self, request, context):
        # 如果不是leader，则转发给leader
        self.client_port = request.clientport
        # logging.info(self.role + ': ' + 'recv PUTDEL from client ' + self.client_port + ' !!!!!!!!!!')
        if self.role =='follower' and self.leader_id:
            self.cmd_output = {}
            self.cmd_output['msgType']='put' if request.opera_type=='put' else 'del'
            self.cmd_output['msg']='received PUT command from client' if request.opera_type=='put' else 'received DEL command from client'
            self.cmd_output['localID']=self.id
            self.cmd_output['leaderID']=self.leader_id
            print(str(self.cmd_output))
            # logging.info(self.role + ': ' + 'PutDelRedirect: client_append_entries to leader ' + self.leader_id)
            with grpc.insecure_channel('localhost:'+str(self.peers[self.leader_id][1])) as channel:
                stub = rpcService_pb2_grpc.RPCStub(channel)
                try:
                    putDelRedirect_response = stub.PutDelRedirect(rpcService_pb2.putDelRedirectRequest(key=request.key, \
                        value=request.value, type=request.type, clientport=request.clientport, opera_type=request.opera_type), self.connect_timeout_in_seconds)
                    # print("\n")
                    self.cmd_output = {}
                    self.cmd_output['msgType']='info'
                    self.cmd_output['msg']='redirect clientRequest to leader success'
                    self.cmd_output['localID']=self.id
                    self.cmd_output['leaderID']=self.leader_id
                    print(str(self.cmd_output))
                    return rpcService_pb2.putDelResponse(success=putDelRedirect_response.success, error_msg=None)
                except:
                    self.cmd_output = {}
                    self.cmd_output['msgType']='info'
                    self.cmd_output['msg']='redirect clientRequest to leader failed, maybe connect error'
                    self.cmd_output['localID']=self.id
                    self.cmd_output['leaderID']=self.leader_id
                    print(str(self.cmd_output))
                    # print("----------------------------putDelRedirectResponse connect error!")
                    # print("\n")
                    return rpcService_pb2.putDelResponse(success=False, error_msg='putDelRedirectResponse connect error')
        elif self.role == 'candidate':
            # logging.info(self.role + ': ' + 'no leader!!!')
            # print("\n")
            self.cmd_output = {}
            self.cmd_output['msgType']='put' if request.opera_type == 'put' else 'del'
            self.cmd_output['msg']='received PUT/DEL command from client, however, there is no leader'
            self.cmd_output['localID']=self.id
            self.cmd_output['leaderID']=self.leader_id
            print(str(self.cmd_output))

            return rpcService_pb2.putDelResponse(success=False, error_msg='no leader!!!')
        else:
            self.cmd_output = {}
            self.cmd_output['msgType']='put' if request.opera_type == 'put' else 'del'
            self.cmd_output['msg']='received PUT command from client' if request.opera_type == 'put' else 'received DEL command from client'
            self.cmd_output['localID']=self.id
            print(str(self.cmd_output))

            self.leader_do(request)
            # print("\n")
            return rpcService_pb2.putDelResponse(success=True, error_msg=None)

    def PutDelRedirect(self, request, context):
        '''
        这个函数只有leader才会调用到
        '''
        self.cmd_output = {}
        self.cmd_output['msgType']='PutDelRedirect'
        self.cmd_output['msg']="received PutDelRedirect command from follower's redirect"
        self.cmd_output['localID']=self.id
        print(str(self.cmd_output))

        self.client_port = request.clientport
        self.leader_do(request)
        # print("\n")
        return rpcService_pb2.putDelRedirectResponse(success=True)

    def all_do(self):
        # logging.info('ENTER all_do')
        # logging.info('check if now need to apply log to state machine!!')
        # logging.info(self.role + ': ' + "sever id: " + self.id + " kv: " + str(self.kv))
        if self.commit_index > self.last_applied:
            self.last_applied = self.commit_index
            # logging.info('all: 1. last_applied = ' + str(self.last_applied))
            # logging.info('all: 2. apply log[last_applied] to kv state machine')
            last_applied = self.log.get_entries(self.log.last_log_index)[0].split(" ") #"255 x 1578762179.732143 client_append_entries"

            opera_type = last_applied[-1]
            key = last_applied[1]
            value = last_applied[2]
            if opera_type == 'put':
                self.kv[key] = value
                self.save()
            elif opera_type == 'del':
                if key in self.kv:
                    self.kv.pop(key)
                self.save()

            self.cmd_output = {}
            self.cmd_output['msgType']='info'
            self.cmd_output['msg']="advance applied log"
            self.cmd_output['localID']=self.id
            self.cmd_output['committedIndex']=self.commit_index
            self.cmd_output['last_applied']=self.last_applied
            self.cmd_output['lastLogIndex']=self.log.last_log_index
            self.cmd_output['lastLogTerm']=self.log.last_log_term
            print(str(self.cmd_output))
            # logging.info(self.role + ': ' + "sever id: " + self.id + " kv: " + str(self.kv))
            # print("\n")

    def follower_do(self, task='becomeCandidate'):
        '''
        task: ['becomeCandidate'], 默认是处理消息
        '''
        self.cmd_output['msgType'] = 'info'
        self.cmd_output['msg'] = 'lost leader and start a new election'
        self.cmd_output['leaderID'] = self.leader_id
        print(str(self.cmd_output))
        self.all_do()

        # 收不到任何消息(leader或者candiate??)，自己变成candidate
        if task == 'becomeCandidate':
            # logging.info('follower：1. become candidate')
            self.cmd_output = {}
            self.cmd_output = {'msgType':'info',
                                'msg':'become candidate'}
            self.next_leader_election_timer_cancel()

            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}
            self.cmd_output['localID'] = self.id
            self.cmd_output['currrentTerm'] = self.current_term
            self.cmd_output['vote_for'] = self.voted_for
            print(str(self.cmd_output))

            # become candidate and begin election timer
            # election timer是为了设定选举时间，以便在超时后在candidate_do内部启动开始新一轮的leader选举
            self.next_leader_election_timer_restart()
            # candidate开始发vote给all other servers
            self.candidate_do(task='sendVoteToPeers')

        # 处理从leader或者candidate发来的消息：这个在append那里已经处理了
        # print("\n")
        return

    def candidate_do(self, task='sendVoteToPeers'):
        '''
        task: ['sendVoteToPeers', 'reElection', 'convertToFollower']
        '''
        self.all_do()

        # 调用rpc向所有peers发送RequestVote请求
        # 并更新voted_for，以及判断是否可以转换身份
        if task == 'sendVoteToPeers':
            for dst_id in self.peers:
                if self.vote_ids[dst_id] == 0:
                    # logging.info('candidate: 1. send request_vote to peer ' + dst_id)
                    self.cmd_output = {}
                    self.cmd_output = {'msgType':'info',
                                    'msg':'send RequestVote',
                                    'localID':self.id}
                    self.cmd_output['to'] = dst_id
                    self.cmd_output['currrentTerm'] = self.current_term
                    self.cmd_output['lastLogTerm'] = self.log.last_log_term
                    self.cmd_output['lastLogIndex'] = self.log.last_log_term
                    print(str(self.cmd_output))

                    peer_addr = str(self.peers[dst_id][0]) + ':' + str(self.peers[dst_id][1])
                    msgReject = True
                    with grpc.insecure_channel(peer_addr) as channel:
                        stub = rpcService_pb2_grpc.RPCStub(channel)
                        try:
                            response = stub.RequestVote(rpcService_pb2.requestVoteRequest(term=self.current_term,
                                                                                            candidateId=self.id,
                                                                                            last_log_index=self.log.last_log_index,
                                                                                            last_log_term=self.log.last_log_term), self.connect_timeout_in_seconds)
                            if response.votedGranted:
                                msgReject=False
                                # logging.info('candidate: 1. recv request_vote_response from follower ' + str(self.peers[dst_id][0]) + ": " + str(self.peers[dst_id][1]))
                            self.vote_ids[response.responserId] = response.votedGranted
                            self.cmd_output = {}
                            self.cmd_output = {'msgType':'MsgVoteResp',
                                            'msg':'received MsgVoteResp',
                                            'localID':self.id,
                                            'currentTerm':self.current_term,
                                            'msgFrom':response.responserId,
                                            'msgReject':msgReject}
                            print(str(self.cmd_output))
                        except:
                            self.cmd_output = {}
                            self.cmd_output = {'msgType':'info',
                                            'msg':'send RequestVote error',
                                            'localID':self.id,
                                            'to':dst_id}
                            print(str(self.cmd_output))

                vote_count = sum(list(self.vote_ids.values()))
                if vote_count >= (len(self.peers)+1)//2:  #2/3 3/4
                    # logging.info('candidate: 2. become leader')
                    self.role = 'leader'
                    self.voted_for = None
                    self.save()
                    self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                    self.match_index = {_id: 0 for _id in self.peers}

                    self.cmd_output = {}
                    self.cmd_output = {'msgType':'info',
                                    'msg':'become Leader',
                                    'localID':self.id}
                    print(str(self.cmd_output))

                    self.next_leader_election_timer_cancel() # 作为leader的时候不需要timeout计时
                    # 成为leader，开始向其他peer发心跳包，不然别的node会一直candidate选举
                    # 且只需要在这里运行一次，next_heartbeat_timer_restart函数内部会进行递归操作
                    self.next_heartbeat_timer_restart()
                    self.cmd_output = {}
                    self.cmd_output = {'msgType':'info',
                                    'msg':'bcastAppend',
                                    'localID':self.id,
                                    'committedIndex':self.commit_index,
                                    'lastApplied':self.last_applied,
                                    'lastLogTerm':self.log.last_log_term,
                                    'lastLogIndex':self.log.last_log_index
                                    }
                    print(str(self.cmd_output))
                    self.leader_do(data=None)

                    # print("\n")
                    return

        elif task=='convertToFollower':
            # logging.info('candidate: 1. recv append_entries from leader ' + self.leader_id)
            # logging.info('candidate: 2. candidate convertToFollower')
            self.cmd_output = {}
            self.cmd_output = {'msgType':'MsgHeartBeat',
                            'msg':'candidate received MsgHeartBeat, become follower',
                            'localID':self.id,
                            'currentTerm':self.current_term}
            print(str(self.cmd_output))
            self.role = 'follower'
            self.voted_for = None
            self.save()
            # print("\n")
            return
        elif task=='reElection':
            self.cmd_output = {}
            self.cmd_output = {'msgType':'info',
                            'msg':'election timeout and start a new election',
                            'localID':self.id}
            print(str(self.cmd_output))
            self.next_leader_election_timer_cancel()
            # logging.info('candidate: 1. leader_election timeout')
            # logging.info('candidate: 2. reElection')
            # logging.info('candidate: 3. become candidate')
            self.next_leader_election_timer_restart()
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}

            # candidate开始发vote给all other servers
            self.candidate_do(task='sendVoteToPeers')
            # print("\n")
            return

    def leader_do(self, data):

        # logging.info('ENTER leader_do')
        self.all_do()

        # 接收来自client的数据：client_append_entries
        if data!=None and data.type=='client_append_entries':
            entry_list=[]
            entry_list.append(str(self.current_term))
            entry_list.append(data.key)
            entry_list.append(data.value)
            entry_list.append(data.type)
            # entry_list.append(data.clientport)
            entry_list.append(data.opera_type)
            entry_str = ' '.join(entry_list)
            self.log.append_entries(self.log.last_log_index, [entry_str])

            # logging.info('leader：1. recv append_entries from client ' + data.clientport)
            # logging.info('leader：2. log append_entries')
            # logging.info('leader：3. log save')

            # print("\n")
            return


        if data !=None and data.type == 'append_entries_response':
            if data.responserTerm==self.current_term:#不管是heartbeat or not都要处理
                # logging.info('leader：1. recv append_entries_response from follower ' + data.responserId)
                if data.success == False:
                    self.next_index[data.responserId] -= 1
                    # logging.info('leader：2. success = False')
                    # logging.info('leader：3. next_index - 1')
                else:
                    self.match_index[data.responserId] = self.next_index[data.responserId]
                    self.next_index[data.responserId] = self.log.last_log_index + 1
                    # logging.info('leader：2. success = True')
                    # logging.info('leader：3. match_index = ' + str(self.match_index[data.responserId]) +  ' next_index = ' + str(self.next_index[data.responserId]))

        # 下面这段是leader把大部分server已经复制到log的数据，提交反馈到client
        while True:
            N = self.commit_index + 1

            count = 0
            for _id in self.match_index: #这里仿佛是next id
                if self.match_index[_id] >= N:
                    count += 1
                if count >= (len(self.peers)+1)//2: #2/3 3/4
                    self.commit_index = N
                    if self.client_port:
                        with grpc.insecure_channel('localhost:'+self.client_port) as channel:
                            stub = rpcService_pb2_grpc.RPCStub(channel)
                            try:
                                response = stub.Apply(rpcService_pb2.applyRequest(commit_index=self.commit_index), self.connect_timeout_in_seconds)
                            except:
                                pass
                    break
            else:
                break