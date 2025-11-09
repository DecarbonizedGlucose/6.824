package raft

// 文件 raftapi/raft.go 定义了 raft 必须向服务器（或测试器）暴露的接口。
// 每个函数的详细说明见下面的注释。
// Make() 创建一个新的 raft 节点，实现 raft 接口。

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"

	"log"
)

func init() {
	labgob.Register(RequestVoteArgs{})
	labgob.Register(RequestVoteReply{})
	labgob.Register(AppendEntriesArgs{})
	labgob.Register(AppendEntriesReply{})
	labgob.Register(LogEntry{})
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Candidate = 0
	Follower  = 1
	Leader    = 2
)

// 一个实现单个 Raft 对等节点的 Go 对象。
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

	// 以下仅领导者使用

	nextIndex  []int // 对于每个服务器，下一条要发送的日志条目的索引
	matchIndex []int // 对于每个服务器，已知的最高日志条目索引
	applyCh    chan raftapi.ApplyMsg
}

// 返回当前终端以及该服务器是否认为自己是领导者。
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

// 将 Raft 的持久状态保存到稳定存储中，以便在崩溃重启后能够恢复。
// 有关持久化内容的描述，请参阅论文中的图 2。
// 在实现快照之前，应将 nil 作为第二个参数传递给 persister.Save()。
// 在实现快照之后，传递当前快照（如果尚无快照，则传递 nil）。
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

// 恢复之前持久化的状态。如果 data 为 nil 或长度小于 1，说明没有持久化状态，
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

// RequestVote RPC 参数结构示例。
// 字段名必须以大写字母开头！
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 当前任期
	CandidateId  int // 发起选票请求的候选者的标识符
	LastLogIndex int // 候选者的最后日志的索引
	LastLogTerm  int // 候选者的最后日志所属任期
}

// RequestVote RPC 回复结构示例。
// 字段名必须以大写字母开头！
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期，用于候选者更新自己的任期
	VoteGranted bool // 对方是否投票给该候选者
}

// RequestVote RPC 处理函数示例。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// 候选者任期落后，拒绝投票
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// 候选者任期比自己高，更新任期并转换为跟随者
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 // 不知道有没有更高任期的Server,故先重置投票状态
	}

	// 检查日志
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	upToDate := false
	if args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		// 任期一致，日志更超前（或等同），且未投票或已投票给该候选者，投票（幂等性）
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeart = time.Now()
		rf.persist()
	} else {
		// 相同任期但已投票给其他候选者，拒绝投票
		reply.Term = rf.currentTerm
	}
}

// 向服务器发送 RequestVote RPC 的示例代码。
// server 是 rf.peers[] 中目标服务器的索引。
// 期望 args 中包含 RPC 参数。
// 使用 RPC 回复填充 *reply，因此调用者应该传递 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须
// 与处理函数中声明的参数类型相同（包括它们是否为指针）。
//
// labrpc 包模拟了一个有损网络，其中服务器可能无法访问，
// 并且请求和回复可能会丢失。Call() 发送请求并等待回复。
// 如果回复在超时间隔内到达，Call() 返回 true；否则，
// Call() 返回 false。因此 Call() 可能暂时不会返回。
// 错误返回可能是由于服务器宕机、无法访问的在线服务器、
// 请求丢失或回复丢失造成的。
//
// Call() 保证返回（可能经过延迟），*除非*服务器端的处理函数
// 没有返回。因此，无需在 Call() 周围实现自己的超时机制。
//
// 更多详细信息，请参阅 ../labrpc/labrpc.go 中的注释。
//
// 如果您在使用 RPC 时遇到问题，请检查是否已将通过 RPC 传递的
// 结构体中的所有字段名称大写，以及调用者是否使用 & 传递回复
// 结构体的地址，而不是结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // 当前任期
	LeaderId     int        // 领导者ID
	PrevLogIndex int        // 新日志追加位置索引的前一个位置
	PrevLogTerm  int        // PrevLogIndex对应的任期
	Entries      []LogEntry // 要追加的日志条目（为空时表示心跳）
	LeaderCommit int        // 领导者已提交的最大索引
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查任期
	if args.Term < rf.currentTerm {
		// 拒绝旧的任期
		reply.Term = rf.currentTerm
		reply.Success = false
		log.Printf("[Server %d][Term %d] Rejecting AppendEntries from Term %d\n", rf.me, rf.currentTerm, args.Term)
		return
	}
	// 遇到新的任期，更新任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		log.Printf("[Server %d][Term %d] Updated to Term %d due to AppendEntries\n", rf.me, rf.currentTerm, args.Term)
	}
	rf.lastHeart = time.Now()
	reply.Term = args.Term

	// 日志一致性检查
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 日志不一致，拒绝
		reply.Success = false
		log.Printf("[Server %d][Term %d] Rejecting AppendEntries due to log inconsistency\n", rf.me, rf.currentTerm)
		return
	}
	// 剩下的情况，日志一致，继续追加
	reply.Success = true
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
	}
	log.Printf("[Server %d][Term %d] Accepted AppendEntries, log now has %d entries\n", rf.me, rf.currentTerm, len(rf.log))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) initLeaderState() {
	// 成为领导者
	rf.state = Leader
	rf.votedFor = -1
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) broadcastHeartbeat() {
	var stay_leader int32 = 1
	for i := range rf.peers {
		if rf.killed() || atomic.LoadInt32(&stay_leader) == 0 {
			break
		}
		if i == rf.me {
			continue
		}
		go func(server int) {
			// 构造参数时读取共享状态需加锁，且保证索引边界安全
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			nextIndex := min(max(rf.nextIndex[server], 0), len(rf.log))
			entries := append([]LogEntry{}, rf.log[nextIndex:]...)
			rf.mu.Unlock()
			args.Entries = entries
			reply := &AppendEntriesReply{}
			log.Printf("[Server %d][Term %d] Leader sending AppendEntries to Server %d, including %d logs\n", rf.me, rf.currentTerm, server, len(args.Entries))
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				// 网络调用失败时不要错误地回退 nextIndex，等待下次重试
				return
			}
			// 核对信息
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Success {
				// 成功：推进 matchIndex 和 nextIndex
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				return
			}
			// 失败：检查任期或回退 nextIndex
			if reply.Term > rf.currentTerm {
				// 观察到更高任期，放弃领导者身份
				log.Printf("[Server %d][Term %d] Observed higher term %d, stepping down\n", rf.me, rf.currentTerm, reply.Term)
				atomic.StoreInt32(&stay_leader, 0)
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				atomic.StoreInt32(&stay_leader, 0)
				return
			}
			if rf.nextIndex[server] > 0 {
				rf.nextIndex[server]--
			}
		}(i)
	}
	if rf.killed() || atomic.LoadInt32(&stay_leader) == 0 {
		return
	}
	rf.mu.Lock()
	// 统计已复制的日志数量，更新 commitIndex
	// oldCommitIndex := rf.commitIndex
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		count := 1 // 包括自己
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			break // 过半即可
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	rf.currentTerm++ // 增加任期
	rf.state = Candidate
	term := rf.currentTerm
	rf.mu.Unlock()

	votes := 1
	rf.votedFor = rf.me
	rf.persist()
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

// 使用 Raft 的服务（例如键值服务器）想要启动协议，确定
// 要追加到 Raft 日志中的下一个命令。如果此服务器不是领导者，
// 则返回 false。否则，启动协议并立即返回。
// 无法保证此命令最终会被提交到 Raft 日志，因为领导者
// 可能失败或选举失败。即使 Raft 实例已被终止，此函数也应正常返回。
// 第一个返回值是命令最终提交后出现的索引。第二个返回值是当前任期。
// 第三个返回值为 true，如果此服务器认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		log.Printf("[Server %d][Term %d] Not leader, cannot Start() command!\n", rf.me, rf.currentTerm)
		return -1, rf.currentTerm, false
	}
	index = len(rf.log)
	term = rf.currentTerm
	newLog := LogEntry{Command: command, Term: term}
	rf.log = append(rf.log, newLog)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	log.Printf("[Leader %d][Term %d] Start() command at index %d: %v\n", rf.me, rf.currentTerm, index, command)
	go rf.broadcastHeartbeat()
	return index, term, isLeader
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			// 有可用日志
			start := rf.lastApplied + 1
			end := rf.commitIndex
			entries := make([]LogEntry, end-start+1)
			copy(entries, rf.log[start:end+1])
			rf.lastApplied = end
			rf.mu.Unlock()
			for i, entry := range entries {
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: start + i,
				}
				log.Printf("[Server %d][Term %d] !!Applied log %d: %+v\n", rf.me, rf.currentTerm, start+i, entry.Command)
			}
		} else {
			rf.mu.Unlock()
			// log.Printf("[Server %d][Term %d] No logs to apply, sleeping\n", rf.me, rf.currentTerm)
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// 测试器不会在每次测试后停止 Raft 创建的 goroutine，
// 但它会调用 Kill() 方法。你的代码可以使用 killed() 来检查 Kill() 是否已被调用。
// 使用原子操作避免了使用锁。问题在于长时间运行的 goroutine 会占用内存，并且可能消耗
// CPU 时间，这可能会导致后续测试失败，并产生令人困惑的调试输出。任何包含
// 长时间运行循环的 goroutine 都应该调用 killed() 来检查是否应该停止。
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
		rf.mu.Lock()
		elapsed := time.Since(rf.lastHeart)
		rf.mu.Unlock()
		if rf.state == Leader {
			// 是领导者，发送心跳
			rf.broadcastHeartbeat()
		} else if elapsed >= time.Duration(ms)*time.Millisecond {
			// 不是leader 且心跳超时, 开始选举
			ok := false
			for !ok && rf.state != Leader && !rf.killed() {
				ok = rf.startElection()
				if !ok {
					// 选举失败，等待一段时间再重试
					ms := 50 + (rand.Int63() % 300)
					time.Sleep(time.Duration(ms) * time.Millisecond)
				} else {
					// 成为领导者，发送心跳
					rf.initLeaderState()
					rf.broadcastHeartbeat()
					// go == true, auto break
					log.Printf("=== Server %d became Leader at Term %d ===", rf.me, rf.currentTerm)
				}
			}
		}
	}
}

// 服务或测试人员想要创建一个 Raft 服务器。所有 Raft 服务器（包括当前服务器）的端口
// 都位于 peers[] 数组中。此服务器的端口是 peers[me]。所有服务器的 peers[] 数组
// 顺序相同。persister 是此服务器保存其持久状态的地方，并且初始状态也保存了
// 最近保存的状态（如果有）。applyCh 是一个通道，测试人员或服务
// 期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应该启动 goroutine来执行任何长时间运行的任务。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0} // 占位，日志索引从1开始
	rf.commitIndex = 0
	rf.lastApplied = 0

	// state int
	rf.state = Follower

	// 仅领导者
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.lastHeart = time.Now()
	go rf.ticker()
	go rf.applyLoop()

	return rf
}
