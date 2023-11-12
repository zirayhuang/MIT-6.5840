# MIT 6.5840 Lab
This code represents the implementation of the Raft consensus protocol in the MIT-6.5840 Distributed Systems lab. 
Each test has been successfully executed 100 times in a row without any failures.
## Lab2A:

### Guidelines for Resetting votedFor to null
1. Reset votedFor to null upon receiving an AppendEntries RPC call with request.Term >= currentTerm.
2. Reset votedFor to null if a response is received with a reply.Term that is greater than the server's current currentTerm
3. If the server does not receive a majority of votes during an election (i.e., it does not get enough votes), the candidate will step down and revert to a follower state, at which point votedFor should be reset to null.

### Implementing Timeout Reset
1. Implement timeout reset functionality using a buffered channel of size one. This channel serves as a reset signal buffer.
2. Within the ticker routine, attempt to receive a signal from the buffered channel within the election timeout period. If no signal is received (indicating a timeout event), the server should initiate a new election.
3. To handle reset events, send a signal to the buffered channel, which the ticker routine will pick up to reset the timer.

### Sending Vote Requests Concurrently
1. Send vote requests in parallel to all servers to optimize the voting process.
2. Upon receiving a vote, a server should record it using a channel dedicated to collecting votes (vote processes share one channel in an election).
3. The candidate server can consider the election won, transitioning to the leader state as soon as it has received a majority of votes, thereby maximizing performance and responsiveness.

## Lab2B:
### Handling Heartbeat Requests for Followers
As mentioned in the student's guide, it's very important that the follower must complete the verification of PrevLogIndex and PrevLogTerm to ensure the logs are consistent with the leader’s and to maintain log consistency.

### Unification of Heartbeat and AppendEntries Implementation

In my  implementation, heartbeats and AppendEntries requests are treated identically. The distinction is made only by the content of the message: a heartbeat is simply an AppendEntries RPC with no log entries attached.
When a leader issues a heartbeat, if there are new log entries to send, they are included in the request. Otherwise, the message functions as a heartbeat.
This unified approach simplifies the implementation and ensures that any necessary log entries are not omitted from heartbeat intervals.
#### Justification with a Three-Node Scenario

Consider a situation involving three nodes with the following log entries:
- Node1: (term 1, index 1, log entry 100), (term 1, index 2, log entry 101)
- Node2: (term 1, index 1, log entry 100), (term 2, index 2, log entry 102)
- Node3: (term 1, index 1, log entry 100), (term 2, index 2, log entry 102)
- Node1, initially the leader for term 1, replicates the entry (term 1, index 1, log entry 100) to Node2 and Node3, which becomes committed.
- After Node1 is disconnected and subsequently receives the entry (term 1, index 2, log entry 101), Node2 becomes the leader for term 2 and replicates a new entry (term 2, index 2, log entry 102), which is also committed.
- When Node2 disconnects and Node1 reconnects, Node3 steps up as the leader for term 3 with a committedIndex of 2.
- As Node3 sends a heartbeat to Node1 with PrevLogIndex = 2 and PrevLogTerm = 2, Node1 steps down due to the term mismatch and returns false for the log matching condition.
- Node3 then adjusts the nextIndex for Node1 to 1 and resends the AppendEntries RPC.
- If Node3 were to send an empty heartbeat instead of including the necessary entry (term 2, index 2, log entry 101), Node1 would not truncate its log due to the empty entries list. However, it would incorrectly commit the entry at index 2 because the heartbeat indicates a committedIndex of 2.


### Ensuring State Consistency Around RPC Calls
It is important to validate the consistency of each variable both before and after the execution of RPC calls. Given the asynchronous nature of these calls, they may execute in an unpredictable order, which can lead to hazardous state mutations if simple increment or decrement operations are applied without checks.

As an example, consider the update operations for nextIndex and matchIndex, an out-of-order RPC response could result in an incorrect adjustment of these indexes if not properly checked.

To address this, a node should:
- Check the term and the validity of the response to ensure it corresponds to the state of the system when the RPC was issued.
- Confirm that any state changes that occur as a result of an RPC are still relevant to the current state before applying them. If the state has progressed beyond the point when the RPC was made, the response should be disregarded to avoid inconsistent state mutations.


## Lab2C
### Optimizations for Log Truncation and Handling Inconsistencies
Codebase from step 2B won't pass the test case TestFigure8Unreliable2C.
To address this, we can integrate the log truncation optimization strategy that is described on pages 7-8 of the original Raft paper, as well as in the guide provided by https://thesquareplanet.com/blog/students-guide-to-raft/

- If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
- If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
- Upon receiving a conflict response, the leader should first search its log for conflictTerm. If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log. 
- If it does not find an entry with that term, it should set nextIndex = conflictIndex.

Don't forget this: If conflictTerm = None, should set nextIndex[peer] to 1.

## Lab2D
- Implemented the InstallSnapshot RPC.
- Introduced persistent variables: lastIncludedIndex, lastIncludedTerm, and snapshot.
- When reading persisted data, initialize `lastApplied` and `committedIndex` to lastIncludedIndex.
- To simplify entry indexing and access, introduced a dummy entry at the beginning of the logs.
- It's possible to get `apply out of order` issue, which is caused by snapshot apply.
  - e.g. follower is behind, InstallSnapshot updates lastApplied[i] to 139, while there's a ongoing AppendEntries applies lower index.
  - Is this acceptable?
