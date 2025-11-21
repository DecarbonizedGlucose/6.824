package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"

	"sync/atomic"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

var idCounter int64 = 0

func genId() int64 {
	return atomic.AddInt64(&idCounter, 1)
}

type Op struct {
	// Field names must start with capital letters
	Me  int   // 客户端id
	Id  int64 // 操作唯一标识符
	Req any   // 客户端请求
}

func (a *Op) equals(b *Op) bool {
	return a.Me == b.Me && a.Id == b.Id
}

type applyResult struct {
	Oper Op  // 应用的操作
	Ret  any // 操作结果
}

// 想要进行自我复制的服务器（例如 ../server.go）会调用 MakeRSM，并且必须实现
// StateMachine 接口。该接口允许 rsm 包与服务器交互，执行服务器特定的操作：
// 服务器必须实现 DoOp 接口来执行操作（例如，Get 或 Put 请求），并实现
// Snapshot/Restore 接口来对服务器状态进行快照和恢复。
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu               sync.Mutex
	me               int
	rf               raftapi.Raft
	applyCh          chan raftapi.ApplyMsg
	maxraftstate     int // snapshot if log grows this big
	servers          []*labrpc.ClientEnd
	sm               StateMachine
	persister        *tester.Persister
	waitCh           map[int]chan applyResult
	appliedResult    map[int]applyResult
	lastAppliedIndex int
}

// servers[] 包含将通过 Raft 协作以构成容错键值服务的服务器集合的端口。
// me 是 servers[] 中当前服务器的索引。
// 键值服务器应通过底层 Raft 实现存储快照，底层 Raft 实现应调用
// persister.SaveStateAndSnapshot() 来原子地保存 Raft 状态以及快照。
// 当 Raft 保存的状态超过 maxraftstate 字节数时，RSM 应进行快照，
// 以便 Raft 可以对其日志进行垃圾回收。如果 maxraftstate 为 -1，则无需进行快照。
// MakerRSM() 必须快速返回，因此它应启动 goroutine 来处理任何长时间运行的任务。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:            me,
		maxraftstate:  maxraftstate,
		servers:       servers,
		applyCh:       make(chan raftapi.ApplyMsg),
		sm:            sm,
		persister:     persister,
		waitCh:        make(map[int]chan applyResult),
		appliedResult: make(map[int]applyResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applyLoop()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) ApplyCh() chan raftapi.ApplyMsg {
	return rsm.applyCh
}

func (rsm *RSM) applyLoop() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.mu.Lock()

			if msg.CommandIndex <= rsm.lastAppliedIndex {
				rsm.mu.Unlock()
				continue
			}
			oper := msg.Command.(Op)
			// 第一次在状态机执行操作
			ret := rsm.sm.DoOp(oper.Req)
			// 缓存结果
			rsm.appliedResult[msg.CommandIndex] = applyResult{Oper: oper, Ret: ret}
			// 唤醒等待这个index的Submit
			if ch, ok := rsm.waitCh[msg.CommandIndex]; ok {
				select {
				case ch <- applyResult{Oper: oper, Ret: ret}:
				default:
				}
				delete(rsm.waitCh, msg.CommandIndex)
			}
			rsm.lastAppliedIndex = msg.CommandIndex
			if rsm.maxraftstate != -1 && rsm.persister.RaftStateSize() > rsm.maxraftstate {
				// 进行快照
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
				for idx := range rsm.appliedResult {
					if idx <= msg.CommandIndex {
						delete(rsm.appliedResult, idx)
					}
				}
			}
			rsm.mu.Unlock()
		} else if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastAppliedIndex = msg.SnapshotIndex
			rsm.appliedResult = make(map[int]applyResult)
			for idx := range rsm.appliedResult {
				if idx <= msg.SnapshotIndex {
					delete(rsm.appliedResult, idx)
				}
			}
			rsm.mu.Unlock()
		}
	}
}

// 向 Raft 提交命令，并等待其被提交。
// 如果客户端找到了新的领导者，则应返回 ErrWrongLeader，并重试。
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	id := genId()
	oper := Op{Me: rsm.me, Id: id, Req: req}
	index, term, isLeader := rsm.rf.Start(oper)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan applyResult, 1)
	rsm.mu.Lock()

	if res, ok := rsm.appliedResult[index]; ok {
		if res.Oper.equals(&oper) {
			rsm.mu.Unlock()
			return rpc.OK, res.Ret
		}
	}
	rsm.waitCh[index] = ch
	rsm.mu.Unlock()

	timeout := time.After(2 * time.Second)
	leaderticker := time.NewTicker(50 * time.Millisecond)

	for {
		select {
		case res := <-ch:
			rsm.mu.Lock()
			delete(rsm.waitCh, index)
			rsm.mu.Unlock()
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || term != currentTerm || res.Oper.Id != id || res.Oper.Me != rsm.me {
				// 领导者变更了
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res.Ret
		case <-leaderticker.C:
			// 定期检查是否还是领导者
			if _, isLeader := rsm.rf.GetState(); !isLeader {
				rsm.mu.Lock()
				delete(rsm.waitCh, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		case <-timeout:
			// 超时
			rsm.mu.Lock()
			delete(rsm.waitCh, index)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}
