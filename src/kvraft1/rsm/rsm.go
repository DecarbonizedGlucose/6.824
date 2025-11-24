package rsm

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int   // 发起请求的服务器 id
	Id  int64 // 每次为一个请求生成一个唯一的 id
	Req any   // 请求内容
}

func opEquals(a *Op, b *Op) bool {
	return a.Me == b.Me && a.Id == b.Id
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

type waitingOp struct {
	oper   Op        // 操作
	result any       // 操作结果
	done   chan bool // 操作完成的信号通道
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	idCounter  int64
	waitingOps map[int]*waitingOp // 正在等待的操作，key 是日志索引
	shutdown   atomic.Bool
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
		idCounter:    0,
		waitingOps:   make(map[int]*waitingOp),
	}
	rsm.shutdown.Store(false)
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if maxraftstate != -1 {
		snapshot := persister.ReadSnapshot()
		if len(snapshot) > 0 {
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var idctr int64
			var smSnapshot []byte
			if d.Decode(&idctr) != nil ||
				d.Decode(&smSnapshot) != nil {
				panic("RSM unable to read snapshot")
			}
			atomic.StoreInt64(&rsm.idCounter, idctr)
			rsm.sm.Restore(smSnapshot)
		}
	}
	go rsm.applyLoop()
	return rsm
}

func (rsm *RSM) genID() int64 {
	return atomic.AddInt64(&rsm.idCounter, 1)
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	if rsm.shutdown.Load() {
		return rpc.ErrWrongLeader, nil
	}

	opID := rsm.genID()
	oper := Op{Me: rsm.me, Id: opID, Req: req}
	index, term, isLeader := rsm.rf.Start(oper)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	waitingOp := &waitingOp{
		oper: oper,
		done: make(chan bool, 1),
	}
	rsm.mu.Lock()
	if wop, exists := rsm.waitingOps[index]; exists {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps[index] = waitingOp
	rsm.mu.Unlock()

	err, result := func() (rpc.Err, any) {
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		for {
			if rsm.shutdown.Load() {
				return rpc.ErrWrongLeader, nil
			}
			select {
			case <-timer.C:
				// 超时，返回错误
				return rpc.ErrWrongLeader, nil
			case <-time.After(300 * time.Millisecond):
				currentTerm, stillLeader := rsm.rf.GetState()
				if !stillLeader || currentTerm != term {
					// 领导者已经变更
					return rpc.ErrWrongLeader, nil
				}
			case res := <-waitingOp.done:
				if res {
					return rpc.OK, waitingOp.result
				} else {
					return rpc.ErrWrongLeader, nil
				}
			}
		}
	}()

	rsm.mu.Lock()
	delete(rsm.waitingOps, index)
	rsm.mu.Unlock()
	return err, result
}

func (rsm *RSM) kill() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.shutdown.Store(true)
	for _, wop := range rsm.waitingOps {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps = make(map[int]*waitingOp)
}

func (rsm *RSM) applyLoop() {
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			rsm.kill()
			return
		}
		if rsm.shutdown.Load() {
			return
		}
		if msg.CommandValid {
			rsm.applyCommand(msg)
		} else {
			rsm.applySnapshot(msg)
		}
	}
}

func (rsm *RSM) applyCommand(msg raftapi.ApplyMsg) {
	oper, ok := msg.Command.(Op)
	if !ok {
		// 非法的操作类型，忽略
		return
	}

	result := rsm.sm.DoOp(oper.Req)

	rsm.mu.Lock()
	if wop, exists := rsm.waitingOps[msg.CommandIndex]; exists {
		if opEquals(&wop.oper, &oper) {
			wop.result = result
			select {
			case wop.done <- true:
			default:
			}
		} else {
			// 操作被覆盖
			select {
			case wop.done <- false:
			default:
			}
		}
	}
	rsm.mu.Unlock()

	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > (rsm.maxraftstate*19)/20 {
		go rsm.createSnapshot(msg.CommandIndex)
	}
}

func (rsm *RSM) applySnapshot(msg raftapi.ApplyMsg) {
	if rsm.shutdown.Load() {
		return
	}
	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)
	var idctr int64
	var smSnapshot []byte
	if d.Decode(&idctr) != nil ||
		d.Decode(&smSnapshot) != nil {
		panic("RSM unable to read snapshot")
	}
	if idctr > atomic.LoadInt64(&rsm.idCounter) {
		atomic.StoreInt64(&rsm.idCounter, idctr)
	}
	rsm.sm.Restore(smSnapshot)
}

func (rsm *RSM) createSnapshot(lastIncludedIndex int) {
	if rsm.shutdown.Load() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	smSnapshot := rsm.sm.Snapshot()
	idctr := atomic.LoadInt64(&rsm.idCounter)
	e.Encode(idctr)
	e.Encode(smSnapshot)
	rsm.rf.Snapshot(lastIncludedIndex, w.Bytes())
}
