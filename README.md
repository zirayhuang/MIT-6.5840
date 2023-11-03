# MIT 6.5840 Lab
## Lab2A:
- When to reset `votedFor` to null?
  - Accepted an AppendEntries 
  - When receive a reply with reply.Term > currentTerm
  - Doesn't get enough votes for a vote, candidate step down to follower, reset votedFor

## Lab2B:
- When follower receive a heartbeat request, after reset timer it should complete the check for PrevLogIndex/PrevLogTerm
- In my implementation, there's no difference between Heartbeat and AppendEntries in terms of implementation, when the leader is trying to send a heartbeat if there's something to be send, just include them in the request, so it's defined as AppendEntries
- Why I want to do this? Imagine the following case with three nodes
  - Node1: (1, 1, 100), (1, 2, 101)
  - Node2: (1, 1, 100), (2, 2, 102)
  - Node3: (1, 1, 100), (2, 2, 101)
  - Node1 was the leader of term1, and it replicates (1,1,100) to Node 2 and 3. (1,1,100) is committed
  - Node1 then disconnect, after this it receives entry (1, 2, 101) from client.
  - Node2 becomes leader, withe term 2, and it received (2,2,102), also replicated to Node3. (2,2,102) is committed.
  - Node2 disconnect
  - Node 1 reconnect
  - Node3 becomes leader, with term3, committedIndex = 2
  - Node3 sends heartbeat to Node1, with PrevLogIndex = 2, PrevLogTerm = 2
  - Node1 receives RPC, steps down. It cannot match PrevLogIndex, return false
  - Node3 decrement nextIndex of Node1 to 1, resend AppendEntries RPC...
  - Now, Node3 should have one (2, 2, 101) to send to Node 1
  - Suppose Node3 send a empty Heartbeat, without the entry, with committed index = 2
  - Node1 will note truncate its log because the entries is empty, but it will committed its entry at index = 2, which is incorrect.

- Check the consistency of every variable before and after RPC call, RPC calls can be out of order, so if it's already changed in another call it's dangerous to simply have decrement or increment operation.
  - Example: update nextIndex, matchIndex 