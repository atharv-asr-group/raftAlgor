# Raft Algorithm with Leader Lease Modification

This project implements a modified Raft system similar to those used by geo-distributed Database clusters like CockroachDB or YugabyteDB. Raft is a consensus algorithm designed for distributed systems to ensure fault tolerance and consistency. It operates through leader election, log replication, and commitment of entries across a cluster of nodes.

## Overview
In this project, a Raft-based distributed key-value store is developed. The key-value pairs map strings (key) to strings (value). The Raft cluster maintains this database, ensuring fault tolerance and strong consistency. The implementation is designed to run on separate Virtual Machines (VMs) hosted on Google Cloud, with gRPC (RECOMMENDED) or ZeroMQ facilitating communication between nodes and client-node interaction.

## Implementation Details

### 1. Storage and Database Operations
The database stores key-value pairs with fault tolerance and replication managed by the Raft algorithm. Each Raft node persists data to logs, ensuring that even after a restart, the node can recover its state. The logs only store WRITE and NO-OP operations.

#### Directory Structure:
```plaintext
assignment/
├─ logs_node_0/
│  ├─ logs.txt
│  ├─ metadata.txt
│  ├─ dump.txt
├─ logs_node_1/
│  ├─ logs.txt
│  ├─ metadata.txt
│  ├─ dump.txt

log.txt contains data in this format:
NO-OP 0
SET name1 Sam 0
SET name2 Drake 0
SET name3 Aujla 1
```

##  Client Interaction
The client interacts with the Raft cluster by sending GET/SET requests to the leader node. The client handles leader failures by updating its leader ID and resending the request to the updated leader until a successful response is received.

## Standard Raft RPCs
Communication between nodes involves two RPCs: AppendEntry and RequestVote, which are essential for log replication and leader election.

## Election Functionalities
Nodes in the Raft cluster implement an election process to choose a leader. The election timeout is randomized, and nodes track the lease duration from previous leaders to ensure a smooth transition.

## Log Replication Functionalities
The leader node sends periodic heartbeats to maintain its leader status and replicates logs across the cluster. If a leader fails to renew its lease, it steps down. Log replication ensures that all nodes in the cluster are synchronized.

## Committing Entries
Entries are committed when a majority of nodes acknowledge appending the entry, ensuring consistency across the cluster.

### Print Statements & Dump.txt
Each node generates a dump.txt file with print statements to indicate the current state and operations performed, aiding in debugging and understanding the system's operation.

#### Example print statements:

"Leader {NodeID} sending heartbeat & Renewing Lease"
"Node {NodeID} became the leader for term {TermNumber}"
"Node {NodeID of follower} committed the entry {entry operation} to the state machine"
### Outro
This project demonstrates the implementation of a fault-tolerant distributed key-value store using a modified Raft consensus algorithm. The use of leader leases adds an additional layer of reliability to the standard Raft protocol, making it suitable for geo-distributed database systems.


## `License`
MIT © Atharv Srivastava 2024<br/>

<p align="Left"> Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: </p> <p align="Left"> The above copyright notice and this permission notice shall be included in all copies or substantial 
    portions of the Software. </p> <p align="Left"> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
      DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. </p> <p align="center"> --- EOF --- </p> 




