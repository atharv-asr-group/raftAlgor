import sys
import random
import concurrent.futures
import threading
import time

import grpc

import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2

HEARTBEAT_DURATION=50
ELECTION_DURATION_FROM=150
ELECTION_DURATION_TO=300

is_terminating=False
is_suspended=False
state_lock=threading.Lock()
election_timer=threading.Event()
heartbeat_events={}
state={
    'election_campaign_timer': None,
    'election_timeout': -1,
    'type': 'follower',
    'nodes': None,
    'term': 0,
    'vote_count': 0,
    'voted_for_id':-1,
    'leader_id': -1,
    'commit_idx':-1,  
    'last_applied':-1,  
    'logs': [],  
    'next_idx':[],  
    'match_idx': [],  
    'replicate_vote_count': 0,
    'data_table':{},  
    'metadata_file_path':None
}


START_TIME=time.time()


def log_prefix():
    time_since_start='{:07.3f}'.format(time.time()- START_TIME)
    return f"{state['term']}\t{time_since_start}\t{state['type']}\t[id={state['id']} leader_id={state['leader_id']} vote_count={state['vote_count']} voted_for={state['voted_for_id']}] "


def write_logs_to_file(node_id):
    with open(f"node_{node_id}_logs.txt", 'w') as f:
        for term, (command, key, value) in state['logs']:
            f.write(f"{term},{command},{key},{value}\n")


def read_logs_from_file(node_id):
    logs=[]
    try:
        with open(f"node_{node_id}_logs.txt", 'r') as f:
            lines=f.readlines()
            for line in lines:
                term, command, key, value=line.strip().split(',')
                logs.append((int(term), (command, key, value)))
    except FileNotFoundError:
        pass  
    return logs



def write_to_node_file(node_id, message):
    with open(f"node_{node_id}_dump.txt", 'a') as f:
        f.write(message + '\n')

def write_heartbeat(node_id):
    message = f"Leader {state['leader_id']} sending heartbeat & Renewing Lease"
    write_to_node_file(node_id, message)

def write_lease_renewal_failure(node_id):
    message = f"Leader {state['leader_id']} lease renewal failed. Stepping Down."
    write_to_node_file(node_id, message)

def write_new_leader_waiting(node_id):
    message = f"New Leader waiting for Old Leader Lease to timeout."
    write_to_node_file(node_id, message)

def write_election_timer_timeout(node_id):
    message = f"Node {node_id} election timer timed out, Starting election."
    write_to_node_file(node_id, message)

def write_node_became_leader(node_id, term_number):
    message = f"Node {node_id} became the leader for term {term_number}."
    write_to_node_file(node_id, message)

def write_rpc_error(node_id, follower_node_id):
    message = f"Error occurred while sending RPC to Node {follower_node_id}."
    write_to_node_file(node_id, message)

def write_follower_committed_entry(node_id, follower_node_id, entry_operation):
    message = f"Node {follower_node_id} (follower) committed the entry {entry_operation} to the state machine."
    write_to_node_file(node_id, message)

def write_leader_received_request(node_id, leader_node_id, entry_operation):
    message = f"Node {leader_node_id} (leader) received an {entry_operation} request."
    write_to_node_file(node_id, message)

def write_leader_committed_entry(node_id, leader_node_id, entry_operation):
    message = f"Node {leader_node_id} (leader) committed the entry {entry_operation} to the state machine."
    write_to_node_file(node_id, message)

def write_follower_accepted_append_entries(node_id, follower_node_id, leader_node_id):
    message = f"Node {follower_node_id} accepted AppendEntries RPC from {leader_node_id}."
    write_to_node_file(node_id, message)

def write_follower_rejected_append_entries(node_id, follower_node_id, leader_node_id):
    message = f"Node {follower_node_id} rejected AppendEntries RPC from {leader_node_id}."
    write_to_node_file(node_id, message)

def write_vote_granted(node_id, candidate_node_id, term_of_vote):
    message = f"Vote granted for Node {candidate_node_id} in term {term_of_vote}."
    write_to_node_file(node_id, message)

def write_vote_denied(node_id, candidate_node_id, term_of_vote):
    message = f"Vote denied for Node {candidate_node_id} in term {term_of_vote}."
    write_to_node_file(node_id, message)

def write_stepping_down(node_id):
    message = f"{node_id} Stepping down"
    write_to_node_file(node_id, message)


def election_timeout_selection():
    return random.randrange(ELECTION_DURATION_FROM, ELECTION_DURATION_TO) * 0.001


def restart_election():
    terminate_election()
    state['election_campaign_timer']=threading.Timer(state['election_timeout'], election_timer.set)
    state['election_campaign_timer'].start()


def select_new_election_timeout_duration():
    state['election_timeout']=election_timeout_selection()


def terminate_election():
    if state['election_campaign_timer']:
        state['election_campaign_timer'].cancel()


def start_election():
    with state_lock:
        state['type']='candidate'
        state['leader_id']=-1
        state['term']+=1
        
        state['vote_count']=1
        state['voted_for_id']=state['id']
        write_state_to_file()

    print(f"I am a candidate. Term: {state['term']}")
    for id in state['nodes'].keys():
        if id != state['id']:
            t=threading.Thread(target=request_vote_thread, args=(id,))
            t.start()
    
    restart_election()


def recieved_majority_votes():
    required_votes=(len(state['nodes']) // 2) + 1
    return state['vote_count']>=required_votes


def recieved_majority_replicate_votes():
    required_votes=(len(state['nodes']) // 2) + 1
    return state['replicate_vote_count']>=required_votes


def finalize_election():
    terminate_election()
    with state_lock:
        if state['type'] !='candidate':
            return

        if recieved_majority_votes():
            state['type']='leader'
            state['leader_id']=state['id']
            state['vote_count']=0
            state['voted_for_id']=-1
            write_state_to_file()

            for i in range(0, len(state['nodes'])):
                if i==state['id']:
                    continue

                state['next_idx'][i]=0
                state['match_idx'][i]=-1

            for id in heartbeat_events:
                heartbeat_events[id].set()

            print("Votes received")
            print(f"I am a leader. Term: {state['term']}")
            write_node_became_leader(state['id'],state['term'])
            
            return
        
        become_follower()
        select_new_election_timeout_duration()
        restart_election()


def become_follower():
    if state['type'] != 'follower':
        print(f"I am a follower. Term: {state['term']}")
    state['type']='follower'
    state['voted_for_id']=-1
    state['vote_count']=0
    write_state_to_file()
    write_stepping_down(state['id'])
    




def request_vote_thread(id_to_request):
    ensure_connected(id_to_request)
    (_, _, stub)=state['nodes'][id_to_request]
    try:
        response=stub.RequestVote(pb2.VoteRequest(
            term=state['term'],
            candidate_id=state['id'],
            last_log_index=len(state['logs']) - 1,
            last_log_term=state['logs'][-1][0] if len(state['logs']) > 0 else -1
        ), timeout=0.1)

        with state_lock:
            
            if state['type'] != 'candidate' or is_suspended:
                return

            if state['term'] < response.term:
                state['term']=response.term
                become_follower()
                restart_election()
            elif response.result:
                state['vote_count'] += 1

        
        if recieved_majority_votes():
            finalize_election()
    except grpc.RpcError:
        reopen_connection(id_to_request)


def election_timeout_thread():
    while not is_terminating:
        if election_timer.wait(timeout=0.5):
            election_timer.clear()
            if is_suspended:
                continue

            
            if state['type']=='follower':
                
                print("The leader is dead")
                write_election_timer_timeout(state['leader_id'])
                
                start_election()
            elif state['type']=='candidate':
                finalize_election()
            


def heartbeat_thread(id_to_request):
    while not is_terminating:
        try:
            if heartbeat_events[id_to_request].wait(timeout=0.5):
                heartbeat_events[id_to_request].clear()

                if (state['type'] !='leader') or is_suspended:
                    continue

                ensure_connected(id_to_request)
                (_, _,stub)=state['nodes'][id_to_request]

                
                response=stub.AppendEntries(pb2.AppendRequest(
                    term=state['term'],
                    leader_id=state['id'],
                    prev_log_index=-404,
                    prev_log_term=-404,
                    entries=None,
                    leader_commit=-404
                ), timeout=0.100)

                if (state['type'] != 'leader') or is_suspended:
                    continue

                with state_lock:
                    if state['term']< response.term:
                        restart_election()
                        state['term']=response.term
                        become_follower()
                threading.Timer(HEARTBEAT_DURATION * 0.001, heartbeat_events[id_to_request].set).start()
        except grpc.RpcError:
            reopen_connection(id_to_request)


def replicate_logs_thread(id_to_request):
    if (state['type'] != 'leader') or is_suspended:
        return

    entries=[]
    idx_from=state['next_idx'][id_to_request]
    for (term, (_, key, value)) in state['logs'][idx_from:]:
        entries.append(pb2.Entry(term=term, key=key, value=value))

    try:
        ensure_connected(id_to_request)

        (_, _, stub)=state['nodes'][id_to_request]
        response=stub.AppendEntries(pb2.AppendRequest(
            term=state['term'],
            leader_id=state['id'],
            prev_log_index=state['next_idx'][id_to_request] - 1,
            prev_log_term=state['logs'][state['next_idx'][id_to_request] - 1][0] if state['next_idx'][id_to_request] > 0 else -1,
            entries=entries,
            leader_commit=state['commit_idx']
        ), timeout=0.100)

        with state_lock:
            if response.result:
                state['next_idx'][id_to_request]=len(state['logs'])
                state['match_idx'][id_to_request]=len(state['logs']) - 1
            else:
                state['next_idx'][id_to_request]=max(state['next_idx'][id_to_request] - 1, 0)
                state['match_idx'][id_to_request]=min(state['match_idx'][id_to_request],
                                                        state['next_idx'][id_to_request] - 1)

    except grpc.RpcError:
        state['next_idx'][id_to_request]=0
        state['match_idx'][id_to_request]=-1
        write_rpc_error(state['id'], id_to_request)
        reopen_connection(id_to_request)




def replicate_logs():
    while not is_terminating:
        time.sleep(0.5)

        if (state['type'] !='leader') or is_suspended or len(state['logs'])==0:
            continue

        with state_lock:
            curr_id=state['id']
            state['match_idx'][state['id']]=len(state['logs']) - 1

        threads=[]
        for node_id in nodes:
            if curr_id==node_id:
                continue

            t=threading.Thread(target=replicate_logs_thread, args=(node_id,))
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()

        with state_lock:
            state['replicate_vote_count']=0
            for i in range(0, len(state['match_idx'])):
                if state['match_idx'][i] >state['commit_idx']:
                    state['replicate_vote_count'] +=1

            if recieved_majority_replicate_votes():
                state['commit_idx']+= 1
                write_state_to_file()

            while state['commit_idx'] >state['last_applied']:
                state['last_applied']+= 1
                _, key, value=state['logs'][state['last_applied']][1]
                state['data_table'][key]=value






class Handler(pb2_grpc.RaftNodeServicer):
    def RequestVote(self, request, context):
        global is_suspended
        if is_suspended:
            return

        restart_election()
        with state_lock:
            if state['term'] <request.term:
                state['term']=request.term
                become_follower()

            failure_reply=pb2.ResultWithTerm(term=state['term'], result=False)
            if request.term < state['term']:
                return failure_reply
            elif request.last_log_index < len(state['logs']) - 1:
                return failure_reply
            elif len(state['logs']) !=0 and request.last_log_index==len(
                    state['logs']) - 1 and request.last_log_term != state['logs'][-1][0]:
                return failure_reply
            elif state['term']==request.term and state['voted_for_id']==-1:
                become_follower()
                state['voted_for_id']=request.candidate_id
                print(f"Voted for node {state['voted_for_id']}")
                write_state_to_file()
                write_vote_granted(state['id'],state['voted_for_id'],state['term'])
                
                return pb2.ResultWithTerm(term=state['term'], result=True)
            write_vote_denied(state['id'], request.candidate_id, request.term)
            return failure_reply

    def AppendEntries(self, request, context):
        global is_suspended
        if is_suspended:
            return

        restart_election()

        with state_lock:
            is_heartbeat=(
                    request.prev_log_index==-404 or
                    request.prev_log_term==-404 or
                    request.leader_commit==-404
            )

            if request.term > state['term']:
                state['term']=request.term
                become_follower()
            if is_heartbeat and request.term==state['term']:
                state['leader_id']=request.leader_id
                return pb2.ResultWithTerm(term=state['term'], result=True)

            failure_reply=pb2.ResultWithTerm(term=state['term'], result=False)
            if request.term < state['term']:
                return failure_reply
            elif request.prev_log_index >len(state['logs']) - 1:
                return failure_reply
            elif request.term==state['term']:
                state['leader_id']=request.leader_id

                success_reply=pb2.ResultWithTerm(term=state['term'], result=True)

                entries=[]
                for entry in request.entries:
                    entries.append((entry.term, ('set', entry.key, entry.value)))

                start_idx=request.prev_log_index + 1

                logs_start=state['logs'][:start_idx]
                logs_middle=state['logs'][start_idx: start_idx+ len(entries)]
                logs_end=state['logs'][start_idx +len(entries):]

                has_conflicts=False
                for i in range(0, len(logs_middle)):
                    if logs_middle[i][0] != entries[i][0]:
                        has_conflicts=True
                        break

                if has_conflicts:
                    state['logs']=logs_start+ entries
                else:
                    state['logs']=logs_start+ entries +logs_end

                if request.leader_commit > state['commit_idx']:
                    state['commit_idx']=min(request.leader_commit, len(state['logs']) - 1)
                    write_state_to_file()

                    while state['commit_idx']> state['last_applied']:
                        state['last_applied'] += 1
                        _, key, value=state['logs'][state['last_applied']][1]
                        state['data_table'][key]=value

                return success_reply

            return failure_reply

    def GetLeader(self, request, context):
        global is_suspended
        if is_suspended:
            return

        if state.get('leader_id') is None:
            return

        (host, port, _)=state['nodes'][state['leader_id']]
        return pb2.GetLeaderReply(leader_id=state['leader_id'], address=f"{host}:{port}")

    

    def GetVal(self, request, context):
        global is_suspended
        if is_suspended:
            return
        write_leader_received_request(state['id'], state['leader_id'], 'GET')
        with state_lock:
            value=state['data_table'].get(request.key)
            success=(value is not None)
            value=value if success else "None"

            return pb2.GetReply(success=success, value=value)

    def SetVal(self, request, context):
        global is_suspended
        if is_suspended:
            return
        write_leader_received_request(state['id'], state['leader_id'], 'SET')
        if state['type'] != 'leader':
            if state['leader_id']==-1:
                return pb2.SetReply(success=False)

            ensure_connected(state['leader_id'])

            (_, _, stub)=state['nodes'][state['leader_id']]
            try:
                response=stub.SetVal(pb2.SetRequest(key=request.key, value=request.value), timeout=0.100)
            except:
                return pb2.SetReply(success=False)

            return response

        with state_lock:
            state['logs'].append((state['term'], ('set', request.key, request.value)))
            return pb2.SetReply(success=True)



def write_state_to_file():
    with open(state['metadata_file_path'], 'w') as f:
        f.write(f"Term: {state['term']}\n")
        f.write(f"Voted For: {state['voted_for_id']}\n")
        f.write(f"Commit Index: {state['commit_idx']}\n")

def load_metadata_from_file():
    try:
        with open(state['metadata_file_path'], 'r') as f:
            lines=f.readlines()
            for line in lines:
                key, value=line.strip().split(": ")
                if key=='Term':
                    state['term']=int(value)
                elif key=='Voted For':
                    state['voted_for_id']=int(value)
                elif key=='Commit Index':
                    state['commit_idx']=int(value)
    except FileNotFoundError:
        pass



def ensure_connected(id):
    if id==state['id']:
        raise "Shouldn't try to connect to itself"
    (host, port, stub)=state['nodes'][id]
    if not stub:
        channel=grpc.insecure_channel(f"{host}:{port}")
        stub=pb2_grpc.RaftNodeStub(channel)
        state['nodes'][id]=(host, port, stub)


def reopen_connection(id):
    if id==state['id']:
        raise "Shouldn't try to connect to itself"
    (host, port, stub)=state['nodes'][id]
    channel=grpc.insecure_channel(f"{host}:{port}")
    stub=pb2_grpc.RaftNodeStub(channel)
    state['nodes'][id]=(host, port, stub)


def start_server(state):
    (ip, port, _stub)=state['nodes'][state['id']]
    server=grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftNodeServicer_to_server(Handler(), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    return server

def main(id, nodes):
    state['logs']=read_logs_from_file(id)
    state['metadata_file_path']=f"metadata_node_{id}.txt"
    load_metadata_from_file()

    election_th=threading.Thread(target=election_timeout_thread)
    election_th.start()

    heartbeat_threads=[]
    for node_id in nodes:
        if id != node_id:
            heartbeat_events[node_id]=threading.Event()
            t=threading.Thread(target=heartbeat_thread, args=(node_id,))
            t.start()
            heartbeat_threads.append(t)

    state['id']=id
    state['nodes']=nodes
    state['type']='follower'
    state['term']=0
    state['next_idx']=[0] * len(state['nodes'])
    state['match_idx']=[-1] * len(state['nodes'])

    log_replication_th=threading.Thread(target=replicate_logs)
    log_replication_th.start()

    server=start_server(state)
    (host, port, _)=nodes[id]
    print(f"The server starts at {host}:{port}")
    print(f"I am a follower. Term: 0")
    select_new_election_timeout_duration()
    restart_election()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        global is_terminating
        is_terminating=True
        server.stop(0)
        print("Shutting down")

        write_logs_to_file(id)

        election_th.join()
        [t.join() for t in heartbeat_threads]



if __name__=='__main__':
    [id]=sys.argv[1:]
    nodes=None
    with open("nodes.conf", 'r') as f:
        line_parts=map(lambda line: line.split(), f.read().strip().split("\n"))
        nodes=dict([(int(p[0]), (p[1], int(p[2]), None)) for p in line_parts])
        print(list(nodes))
    main(int(id), nodes)