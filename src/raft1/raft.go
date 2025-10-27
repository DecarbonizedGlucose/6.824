package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.
//
// 翻译：文件 raftapi/raft.go 定义了 raft 必须向服务器（或测试器）暴露的接口。
// 每个函数的详细说明见下面的注释。
// Make() 创建一个新的 raft 节点，实现 raft 接口。

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

const (
	Candidate = 0
	Follower  = 1
	Leader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int        // 当前任期
	votedFor    int        // 投票给谁
	log         []LogEntry // 日志条目
	commitIndex int        // 已提交的最高日志条目索引
	lastApplied int        // 已应用到状态机的最高日志条目索引

	state     int       // 节点状态：候选者、领导者、跟随者
	lastHeart time.Time // 最后收到心跳时间

	// 仅领导者
	nextIndex  []int // 对于每个服务器，下一条要发送的日志条目的索引
	matchIndex []int // 对于每个服务器，已知的最高日志条目索引
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
//
// 翻译：将 Raft 的持久化状态保存到稳定存储，以便在崩溃和重启后恢复。
// 参考论文图 2 了解应当持久化的内容。在实现快照之前，向 persister.Save()
// 的第二个参数传入 nil；实现快照后传入当前快照（若尚无快照则传入 nil）。
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
//
// 翻译：恢复之前持久化的状态。如果 data 为 nil 或长度小于 1，说明没有持久化状态，
// 可以直接返回。这里应从 data 解码出之前保存的字段并恢复到 Raft 实例中。
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

// 返回 Raft 已持久化状态占用的字节数（可用于判断何时需要做快照）。
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 服务表示它已经创建了包含直到并包括 index 的快照，这意味着服务
// 不再需要包含直到该索引（包含）的日志条目。Raft 应尽可能修剪日志以节省空间。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 当前任期
	CandidateId  int // 发起选票请求的候选者的标识符
	LastLogIndex int // 候选者的最后日志的索引
	LastLogTerm  int // 候选者的最后日志所属任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期，用于候选者更新自己的任期
	VoteGranted bool // 对方是否投票给该候选者
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// 候选者任期落后，拒绝投票
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		// 候选者任期比自己高，更新任期并转换为跟随者
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.lastHeart = time.Now()
		rf.persist()
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 检查日志（暂时忽略）
		// ...
		// 任期一致，且未投票或已投票给该候选者，投票（幂等性）
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeart = time.Now()
		rf.persist()
	}
}

// 向服务器发送 RequestVote RPC 的示例代码。
// server 是 rf.peers[] 中目标服务器的索引。
// 期望 args 中包含 RPC 参数。
// 使用 RPC 回复填充 *reply，因此调用者应该
// 传递 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须
// 与处理函数中声明的参数类型相同（包括它们是否为指针）。
//
// labrpc 包模拟了一个有损网络，其中服务器
// 可能无法访问，并且请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果回复在
// 超时间隔内到达，Call() 返回 true；否则，
// Call() 返回 false。因此 Call() 可能暂时不会返回。
// 错误返回可能是由于服务器宕机、
// 无法访问的在线服务器、请求丢失或回复丢失造成的。
//
// Call() 保证返回（可能经过延迟），*除非*服务器端的处理函数
// 没有返回。因此，
// 无需在 Call() 周围实现自己的超时机制。
//
// 更多详细信息，请参阅 ../labrpc/labrpc.go 中的注释。
//
// 如果您在使用 RPC 时遇到问题，请检查
// 是否已将通过 RPC 传递的结构体中的所有字段名称大写，以及
// 调用者是否使用 & 传递回复结构体的地址，而不是
// 结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct{}

type AppendEntriesReply struct{}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) initLeaderState() {
	// 成为领导者
	rf.state = Leader
	rf.votedFor = -1
	// 初始化领导者状态
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) broacastHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.sendAppendEntries(server, &AppendEntriesArgs{}, &AppendEntriesReply{})
		}(i)
	}
}

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	rf.currentTerm++ // 增加任期
	rf.state = Candidate
	term := rf.currentTerm
	rf.mu.Unlock()

	votes := 1
	n := len(rf.peers)
	ch := make(chan bool, n-1) // 带缓冲的通道，防止阻塞

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				ch <- false
				return
			}

			// 观察到更高任期，放弃选举
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				ch <- false
				return
			}
			rf.mu.Unlock()
			ch <- reply.VoteGranted
		}(i)
	}
	for i := 0; i < n-1; i++ {
		if <-ch {
			votes++
			if votes > n/2 {
				// 取得多数票，成为 leader
				rf.mu.Lock()
				rf.initLeaderState()
				rf.mu.Unlock()
				return true
			}
		}
	}
	return votes > n/2
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
//
// 翻译：使用 Raft 的服务（例如 k/v 服务器）希望启动对下一个要追加到 Raft 日志的命令达成一致。
// 如果该服务器不是 leader，则返回 false。否则开始该一致性操作并立即返回。
// 无法保证该命令最终会被提交到 Raft 日志，因为 leader 可能失败或在选举中落败。
// 即使 Raft 实例已被 Kill()，此函数也应优雅返回。
//
// 返回值说明：第一个返回值是命令被提交后会出现的索引（如果提交）；第二个是当前任期；
// 第三个在服务器自认为是 leader 时为 true。
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
//
// 翻译：测试器在每次测试后不会停止 Raft 创建的 goroutine，但会调用 Kill()。
// 你的代码可以使用 killed() 检查是否已调用 Kill()；使用 atomic 可以避免加锁。
// 长时间运行的 goroutine 会占用内存和 CPU，可能导致后续测试失败并产生混乱的调试信息，
// 所以带有长循环的 goroutine 应该周期性检查 killed() 并在需要时退出。
func (rf *Raft) Kill() {
	if !rf.killed() {
		atomic.StoreInt32(&rf.dead, 1)
		// 如果需要的话，请在此处输入您的代码。
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// 您的代码在此处 (3A)
		// 检查领导者选举是否应该开始.
		// 暂停 50 到 350 毫秒之间的随机时间。
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		elapsed := time.Since(rf.lastHeart)
		if rf.state != Leader && elapsed >= time.Duration(ms)*time.Millisecond {
			// 超时, 开始选举
			rf.startElection()
		}
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
//
// 翻译：服务或测试器请求创建一个 Raft 服务器。所有 Raft 服务器（包括本节点）的端点
// 在 peers[] 中，本节点是 peers[me]。所有服务器的 peers[] 的顺序一致。
// persister 用于保存持久化状态，并在初始化时包含最近保存的状态（如果有）。
// applyCh 是测试器或服务期望 Raft 在其上发送 ApplyMsg 的通道。
// Make() 应快速返回，任何长时间运行的工作都应在 goroutine 中启动。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// state int
	rf.state = Follower

	// 仅领导者
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.lastHeart = time.Now()
	go rf.ticker()

	return rf
}
