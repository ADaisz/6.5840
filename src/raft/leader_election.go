package raft

import (
	"math/rand"
	"time"
)

const baseElectionTimeout = 300
const None = -1

func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f

}

// StartElection 中已经加锁了
func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	DPrintf("[server%d] :选举的超时时间设置为: %d", rf.me, rf.electionTimeout)
}

// StartElection 中已经加锁了
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf("[server%d] : start election , item = %d,state =  %d", rf.me, rf.currentTerm, rf.state)
}

func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	rf.becomeCandidate()

	done := false // TODO: done 的作用
	votes := 1    //记录投票总数，初始时，自己为自己投票
	term := rf.currentTerm

	args := RequestVoteArgs{rf.currentTerm, rf.me}
	for i, _ := range rf.peers {
		// 跳过 自身节点
		if rf.me == i {
			continue
		}
		go func(serverId int) {
			var reply RequestVoteReply
			// DPrintf("term [server%d],[server%d] send vote request to [server%d]", rf.currentTerm, rf.me, serverId)
			ok := rf.sendRequestVote(serverId, &args, &reply)

			// 丢弃无效票
			if !ok || !reply.VoteGranted {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm > reply.Term {
				return
			}
			//统计票数
			votes++
			// DPrintf("[server%d]获得的投票个数: %d", rf.me, votes)
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true

			//TODO:
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}

			rf.state = Leader
			// 发送心跳

			go rf.StartAppendEntries(true)
		}(i)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("[server%d] term < [server%d] term,so reject", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm

	// 投过给一个人之后，需要更改自身的投票选项，以便不再投票给其他人
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer()
		reply.VoteGranted = true
		DPrintf("[server%d]:我已经投票给 [server%d],它的任期是 %d", rf.me, args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
