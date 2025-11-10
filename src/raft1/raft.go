package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	// Leader 心跳间隔，需小于选举超时时间
	HeartbeatInterval = 100 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	CurrentTerm int        // 服务器已知的最新任期 (持久化)
	VotedFor    int        // 在当前任期内投票给哪个 Candidate (持久化)
	Log         []LogEntry // 日志条目数组 (持久化，3A 仅需其长度和末尾 Term)
	State       State      // 服务器当前状态 (非持久化)

	// Leader 状态 (3A 仅需 NextIndex 和 MatchIndex 的初始化)
	NextIndex  []int // 对于每个服务器，要发送给它的下一个日志条目的索引
	MatchIndex []int // 对于每个服务器，已知已复制到它的最高日志条目的索引

	// 计时器
	ElectionTimer *time.Timer // 用于选举超时的计时器
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.CurrentTerm
	isleader := (rf.State == Leader)
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getRandomElectionTimeout() time.Duration {
	// 随机超时时间在 300ms 到 600ms 之间
	t := 300 + (rand.Int63() % 300)
	return time.Duration(t) * time.Millisecond
}

func (rf *Raft) becomeFollower(term int) {
	rf.State = Follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.ElectionTimer.Reset(rf.getRandomElectionTimeout())
}

func (rf *Raft) becomeLeader() {
	rf.State = Leader
	lastLogIndex := len(rf.Log) - 1
	for i := range rf.peers {
		rf.NextIndex[i] = lastLogIndex + 1
		rf.MatchIndex[i] = 0
	}

	rf.sendHeartbeats()
	rf.ElectionTimer.Reset(HeartbeatInterval)
}

func (rf *Raft) sendHeartbeats() {
	if rf.State != Leader {
		return
	}

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()

				prevLogIndex := rf.NextIndex[server] - 1
				prevLogTerm := rf.Log[prevLogIndex].Term

				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      []LogEntry{}, // 心跳
					LeaderCommit: 0,            // 3A 中 CommitIndex 恒为 0
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.CurrentTerm {
						rf.becomeFollower(reply.Term)
						return
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) startElection() {
	rf.State = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me // 给自己投票

	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.CurrentTerm {
						rf.becomeFollower(reply.Term)
						return
					}

					if rf.State == Candidate && reply.Term == rf.CurrentTerm {
						if reply.VoteGranted {
							votesReceived++
							if votesReceived > len(rf.peers)/2 {
								rf.becomeLeader()
							}
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		// becomeFollower 会更新任期并重置计时器
		rf.becomeFollower(args.Term)
	} else {
		// 收到当前任期 Leader 的心跳（已是 Follower），只需重置计时器
		rf.ElectionTimer.Reset(rf.getRandomElectionTimeout())

		if rf.State == Candidate {
			// 收到当前任期的 Leader 心跳，退位为 Follower
			rf.State = Follower
		}
	}

	reply.Term = rf.CurrentTerm

	lastLogIndex := len(rf.Log) - 1
	if args.PrevLogIndex > lastLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.CurrentTerm

	canVote := (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID)

	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term

	logOk := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if canVote && logOk {
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		rf.ElectionTimer.Reset(rf.getRandomElectionTimeout())
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		select {
		case <-rf.ElectionTimer.C: // 选举超时
			rf.mu.Lock()
			if rf.State == Leader {
				// Leader 超时，发送心跳
				rf.sendHeartbeats()
				// 重置 Leader 心跳计时器
				rf.ElectionTimer.Reset(HeartbeatInterval)
			} else {
				// Follower/Candidate 超时，开始选举
				rf.startElection()
				// 重置选举计时器
				rf.ElectionTimer.Reset(rf.getRandomElectionTimeout())
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	// 初始日志包含一个虚拟条目，索引为 0，任期为 0
	rf.Log = append(rf.Log, LogEntry{Term: 0})

	rf.State = Follower

	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	// 初始化选举计时器
	rf.ElectionTimer = time.NewTimer(rf.getRandomElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
