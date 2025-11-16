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

	"bytes"
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
	mu                sync.Mutex            // mutex锁
	peers             []*labrpc.ClientEnd   // 所有对等节点的RPC端点
	persister         *tester.Persister     // 持久化状态的对象
	me                int                   // 本节点的在peers里的索引
	dead              int32                 // 该节点是否已被杀死
	currentTerm       int                   // 当前任期(Persistent)
	votedFor          int                   // 投票给谁(Persistent)
	log               []LogEntry            // 日志条目(Persistent)
	commitIndex       int                   // 已提交的最高日志条目绝对索引
	lastApplied       int                   // 已应用到状态机的最高日志条目绝对索引
	state             int                   // 节点状态：候选者、领导者、跟随者
	lastHeart         time.Time             // 最后收到心跳时间
	nextIndex         []int                 // 对于每个服务器，下一条要发送的日志条目的绝对索引
	matchIndex        []int                 // 对于每个服务器，已知的最高日志条目绝对索引
	applyCh           chan raftapi.ApplyMsg // 提交日志的通道
	lastIncludedIndex int                   // 快照包含的最后日志的绝对索引
	lastIncludedTerm  int                   // 快照包含的最后日志的任期
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
}

func (rf *Raft) getRaftStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// 将 Raft 的持久状态保存到稳定存储中，以便在崩溃重启后能够恢复。
func (rf *Raft) persist() {
	rf.persister.Save(rf.getRaftStateBytes(), rf.persister.ReadSnapshot())
}

// 恢复之前持久化的状态。如果 data 为 nil 或长度小于 1，说明没有持久化状态，
// 可以直接返回。这里应从 data 解码出之前保存的字段并恢复到 Raft 实例中。
func (rf *Raft) readPersist(data []byte) {
	// if data == nil || len(data) < 1 { // bootstrap without any state?
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}
}

// 返回 Raft 已持久化状态占用的字节数（可用于判断何时需要做快照）。
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Term              int    // 当前任期
	LeaderId          int    // 领导者ID
	LastIncludedIndex int    // 快照包含的最后日志的索引
	LastIncludedTerm  int    // 快照包含的最后日志的任期
	Data              []byte // 快照数据
}

type InstallSnapshotReply struct {
	Term int // 当前任期，用于领导者更新自己的任期
}

// 将相对索引转换为绝对索引
func (rf *Raft) getAbsPos(relIndex int) int {
	return relIndex + rf.lastIncludedIndex
}

// 将绝对索引转换为相对索引
func (rf *Raft) getRelPos(absIndex int) int {
	return absIndex - rf.lastIncludedIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	// 检查任期
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	toPersist := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		toPersist = true
	}
	if rf.state != Follower {
		rf.state = Follower
		rf.votedFor = -1
		toPersist = true
	}
	rf.lastHeart = time.Now()
	if toPersist {
		rf.persist()
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 本地快照更新更快，忽略
		rf.mu.Unlock()
		return
	}

	// 修剪日志
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: args.LastIncludedTerm}
	// 如果快照并未包含所有日志，保留快照之后的日志
	if args.LastIncludedIndex >= rf.lastIncludedIndex && rf.getRelPos(args.LastIncludedIndex) < len(rf.log) {
		relPos := rf.getRelPos(args.LastIncludedIndex)
		newLog = append(newLog, rf.log[relPos+1:]...)
	}
	rf.log = newLog

	// 更新快照元信息
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// 写入新的快照并持久化
	rf.persister.Save(rf.getRaftStateBytes(), args.Data)

	// 更新 commitIndex 和 lastApplied
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.mu.Unlock()

	// 应用新快照到状态机
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || rf.state != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		// 观察到更高任期，放弃领导者身份
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.lastHeart = time.Now()
		return
	}
	// 更新 nextIndex 和 matchIndex
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex
}

// 服务表示它已经创建了包含直到并包括 index 的快照，这意味着服务
// 不再需要包含直到该索引（包含）的日志条目。Raft 应尽可能修剪日志以节省空间。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.getAbsPos(len(rf.log)-1) {
		// 本地日志已被快照覆盖，或索引越界
		return
	}
	logPos := rf.getRelPos(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[logPos].Term
	// 修剪日志
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: rf.lastIncludedTerm}
	// 如果快照并未包含所有日志，保留快照之后的日志
	if index < rf.getAbsPos(len(rf.log)-1) {
		newLog = append(newLog, rf.log[logPos+1:]...)
	}
	rf.log = newLog

	rf.commitIndex = max(index, rf.commitIndex)
	rf.lastApplied = max(index, rf.lastApplied)
	if rf.state == Leader {
		for i := range rf.peers {
			if rf.nextIndex[i] <= index {
				rf.nextIndex[i] = index + 1
			}
			if rf.matchIndex[i] < index {
				rf.matchIndex[i] = index
			}
		}
	}

	rf.persister.Save(rf.getRaftStateBytes(), snapshot)
}

type RequestVoteArgs struct {
	Term         int // 当前任期
	CandidateId  int // 发起选票请求的候选者的标识符
	LastLogIndex int // 候选者的最后日志的索引
	LastLogTerm  int // 候选者的最后日志所属任期
}

type RequestVoteReply struct {
	Term        int  // 当前任期，用于候选者更新自己的任期
	VoteGranted bool // 对方是否投票给该候选者
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// 候选者任期落后，拒绝投票
		return
	}

	if args.Term > rf.currentTerm {
		// 候选者任期比自己高，更新任期并转换为跟随者
		rf.state = Follower
		rf.votedFor = -1 // 不知道有没有更高任期的Server,故先重置投票状态
		rf.currentTerm = args.Term
		rf.persist()
	}

	// 检查日志
	lastLogIndex := rf.getAbsPos(len(rf.log) - 1)
	lastLogTerm := rf.log[len(rf.log)-1].Term
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		// 任期一致，日志更超前（或等同），且未投票或已投票给该候选者，投票（幂等性）
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	}
}

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
	Term          int  // 当前任期，用于领导者更新自己的任期
	Success       bool // 是否成功追加日志
	ConflictTerm  int  // 冲突日志的任期(用于快速回退)
	ConflictIndex int  // 冲突日志的起始索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// 检查任期
	if args.Term < rf.currentTerm {
		// 拒绝旧的任期
		reply.Success = false
		return
	}
	// 遇到新的任期，或者相同任期的心跳，退回至跟随者
	toPersist := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		toPersist = true
	}
	if rf.state != Follower {
		rf.state = Follower
		rf.votedFor = -1
		toPersist = true
	}
	rf.lastHeart = time.Now()
	if toPersist {
		rf.persist()
	}

	// 日志一致性检查
	if args.PrevLogIndex >= rf.getAbsPos(len(rf.log)) {
		// Follower 日志太短
		reply.Success = false
		reply.ConflictIndex = rf.getAbsPos(len(rf.log))
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// Follower 日志被快照覆盖，无法匹配
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.log[rf.getRelPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// 任期不匹配，找到冲突任期在本地首次出现的位置
		reply.Success = false
		conflictTerm := rf.log[rf.getRelPos(args.PrevLogIndex)].Term
		idx := args.PrevLogIndex
		for rf.getRelPos(idx) >= 1 && rf.log[rf.getRelPos(idx-1)].Term == conflictTerm {
			idx--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = idx
		return
	}
	// 剩下的情况，日志一致，继续追加
	reply.Success = true
	if len(args.Entries) != 0 {
		rf.log = rf.log[:rf.getRelPos(args.PrevLogIndex)+1] // 去除有矛盾的日志
		rf.log = append(rf.log, args.Entries...)            // 追加新日志
		rf.persist()
	}
	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, rf.getAbsPos(len(rf.log)-1)))
	}
}

func (rf *Raft) sendAppendEntries(server int, stay_leader *int32) {
	// 构造参数时读取共享状态需加锁，且保证索引边界安全
	rf.mu.Lock()
	if rf.state != Leader {
		atomic.StoreInt32(stay_leader, 0)
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		// 需要发送快照
		rf.mu.Unlock()
		rf.sendInstallSnapshot(server)
		return
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[rf.getRelPos(args.PrevLogIndex)].Term
	nextIndex := min(rf.nextIndex[server], rf.getAbsPos(len(rf.log)))
	entries := []LogEntry{}
	if rf.getRelPos(nextIndex) < len(rf.log) {
		entries = append(entries, rf.log[rf.getRelPos(nextIndex):]...)
	}
	rf.mu.Unlock()
	args.Entries = entries
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		// 网络调用失败时不要错误地回退 nextIndex，等待下次重试
		return
	}
	// 核对信息
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm || rf.state != Leader {
		// 数据保护：任期变化，放弃领导者身份
		atomic.StoreInt32(stay_leader, 0)
		return
	}
	if reply.Success {
		// 成功：推进 matchIndex 和 nextIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// 立即推进commitIndex
		lastIndex := rf.getAbsPos(len(rf.log) - 1)
		for N := lastIndex; N > rf.commitIndex; N-- {
			if N <= rf.lastIncludedIndex {
				// 快照已经提交
				break
			}
			rel := rf.getRelPos(N)
			if rel < 0 || rel >= len(rf.log) || rf.log[rel].Term != rf.currentTerm {
				continue
			} // 只考虑本任期内的
			count := 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				break // 过半即可
			}
		}
		return
	}
	// 失败
	if reply.Term > rf.currentTerm {
		// 观察到更高任期，放弃领导者身份
		atomic.StoreInt32(stay_leader, 0)
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.lastHeart = time.Now()
		atomic.StoreInt32(stay_leader, 0)
		return
	}
	// 回退 nextIndex
	if reply.ConflictTerm != -1 {
		// 找到冲突任期的起始位置
		lastIndex := -1
		for i := len(rf.log) - 1; i >= 1; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				lastIndex = i
				break
			}
		}
		if lastIndex != -1 {
			rf.nextIndex[server] = rf.getAbsPos(lastIndex) + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	} else {
		// 对方日志太短
		rf.nextIndex[server] = reply.ConflictIndex
	}
	// 已经退到不能用日志追加的地步了
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		go rf.sendInstallSnapshot(server)
		return
	}
}

func (rf *Raft) initLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Leader
	rf.votedFor = -1
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getAbsPos(len(rf.log))
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getAbsPos(len(rf.log) - 1)
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
		go rf.sendAppendEntries(i, &stay_leader)
		if rf.killed() || atomic.LoadInt32(&stay_leader) == 0 {
			break
		}
	}
}

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	rf.currentTerm++ // 增加任期
	rf.state = Candidate
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()
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
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: rf.getAbsPos(len(rf.log) - 1),
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
				rf.initLeaderState()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	rf.lastHeart = time.Now()
	index := rf.getAbsPos(len(rf.log))
	term := rf.currentTerm
	newLog := LogEntry{Command: command, Term: term}
	rf.log = append(rf.log, newLog)
	rf.persist()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	go rf.broadcastHeartbeat()
	return index, term, true
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.lastApplied < rf.lastIncludedIndex {
			// 快照未应用
			rf.lastApplied = rf.lastIncludedIndex
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		if rf.commitIndex > rf.lastApplied {
			// 有可用日志
			start := rf.lastApplied + 1
			// 保证安全访问
			if start <= rf.lastIncludedIndex {
				start = rf.lastIncludedIndex + 1
			}
			if rf.getRelPos(start) < 0 || rf.getRelPos(start) >= len(rf.log) {
				rf.mu.Unlock()
				continue
			}
			end := rf.commitIndex
			entries := make([]LogEntry, end-start+1)
			copy(entries, rf.log[rf.getRelPos(start):rf.getRelPos(end)+1])
			rf.lastApplied = end
			rf.mu.Unlock()
			for i, entry := range entries {
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: start + i,
				}
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

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
		rf.mu.Lock()
		state := rf.state
		lastHeart := rf.lastHeart
		rf.mu.Unlock()

		switch state {
		case Leader:
			rf.broadcastHeartbeat()
			time.Sleep(time.Duration(120+rand.Int63n(30)) * time.Millisecond)

		default:
			timeout := time.Duration(150+rand.Int63n(150)) * time.Millisecond
			time.Sleep(timeout / 10)

			rf.mu.Lock()
			elapsed := time.Since(lastHeart)
			// 选举超时
			if elapsed >= timeout && rf.state != Leader {
				rf.mu.Unlock()
				rf.startElection()
				// 不再长循环，下次ticker继续检查
			} else {
				rf.mu.Unlock()
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0} // 占位，日志索引从1开始
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// 初始化持久化状态
	rf.readPersist(persister.ReadRaftState())

	// 后台goroutine
	rf.lastHeart = time.Now()
	go rf.ticker()
	go rf.applier()

	return rf
}
