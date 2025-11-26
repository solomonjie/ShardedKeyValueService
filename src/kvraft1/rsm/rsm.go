package rsm

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Req any
	Id  int64
}

type OpResult struct {
	Value any
	OpId  int64
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	notifyChs   map[int]chan OpResult
	currentTerm int // 用于记录当前的任期，检测变化
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		notifyChs:    make(map[int]chan OpResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	term, _ := rsm.rf.GetState()
	rsm.currentTerm = term

	go rsm.applier()
	go rsm.monitor()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	op := Op{
		Req: req,
		Id:  nrand(),
	}

	index, _, isLeader := rsm.rf.Start(op)

	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	ch := make(chan OpResult, 1)
	rsm.notifyChs[index] = ch
	rsm.mu.Unlock()

	res := <-ch

	defer func() {
		rsm.mu.Lock()
		delete(rsm.notifyChs, index)
		rsm.mu.Unlock()
	}()

	if res.OpId == op.Id {
		return rpc.OK, res.Value
	}

	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			resValue := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			if ch, ok := rsm.notifyChs[msg.CommandIndex]; ok {
				select {
				case ch <- OpResult{Value: resValue, OpId: op.Id}:
				default:
				}
			}
			rsm.mu.Unlock()

			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			}

		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
		}
	}
}

func (rsm *RSM) monitor() {
	for {
		time.Sleep(100 * time.Millisecond)

		term, isLeader := rsm.rf.GetState()

		rsm.mu.Lock()
		if !isLeader || term != rsm.currentTerm {
			rsm.currentTerm = term

			for _, ch := range rsm.notifyChs {
				select {
				case ch <- OpResult{OpId: -1}:
				default:
				}
			}
		}
		rsm.mu.Unlock()
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
