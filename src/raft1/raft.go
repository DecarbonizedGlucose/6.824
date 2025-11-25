package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Leader int = iota
	Candidate
	Follower
)

type LogEntry struct {
	Command any
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // 状态锁
	peers     []*labrpc.ClientEnd // 对等节点RPC端点
	persister *tester.Persister   // 持久化存储
	me        int                 // 本节点在peers中的索引
	dead      int32               // 是否被杀死, 由Kill()设置

	currentTerm int        // 当前任期
	votedFor    int        // 投票给了谁
	log         []LogEntry // 日志条目
	commitIndex int        // 已提交的最高日志条目的索引
	lastApplied int        // 已经被应用到状态机的最高日志条目的索引
	nextIndex   []int      // 对于每个服务器，要发送给他的下一个日志条目的索引
	matchIndex  []int      // 对于每个服务器，已知的最高日志条目的索引

	state            int                   // 当前身份
	electionCh       chan struct{}         // 选举超时通知
	resetELTimeoutCh chan struct{}         // 重置选举超时
	heartbeatCh      chan struct{}         // 心跳超时通知
	resetHBTimeoutCh chan struct{}         // 重置心跳超时 / 立即心跳
	killCh           chan struct{}         // 通知被杀死的通道
	applyCh          chan raftapi.ApplyMsg // 应用日志条目到状态机的通道
	applyCond        *sync.Cond            // 应用日志条目条件变量

	lastIncludedIndex int // 包含在快照中的最高日志条目的索引
	lastIncludedTerm  int // 包含在快照中的最高日志条目的任期
}

/* ==================== utils ==================== */

func (rf *Raft) getAbsPos(relPos int) int {
	return relPos + rf.lastIncludedIndex
}

func (rf *Raft) getRelPos(absPos int) int {
	return absPos - rf.lastIncludedIndex
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	if index < rf.lastIncludedIndex || rf.getRelPos(index) >= len(rf.log) {
		return -1
	}
	return rf.log[rf.getRelPos(index)].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
}

// 需要在锁内调用
func (rf *Raft) initLeaderState() {
	rf.state = Leader
	rf.votedFor = -1
	lastLogIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastLogIndex
}

// 需要在锁内调用
func (rf *Raft) initFollowerState(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist()
}

// 需要在锁内调用
func (rf *Raft) initCandidateState() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
}

/* ==================== Persistence ==================== */

func (rf *Raft) encodedState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodedState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.Save(rf.encodedState(), snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
	}
}

// how many bytes in Raft's persisted log?
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.initFollowerState(args.Term)
	}
	rf.resetElectionTimer()
	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		// snapshot is older than existing one
		rf.mu.Unlock()
		return
	}

	// cut log
	newLog := []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	// save log entries after LastIncludedIndex
	if args.LastIncludedIndex <= rf.getLastLogIndex() {
		relPos := rf.getRelPos(args.LastIncludedIndex)
		newLog = append(newLog, rf.log[relPos+1:]...)
	}
	rf.log = newLog

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	// persist state and snapshot
	rf.persistWithSnapshot(args.Data)

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
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
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm || rf.state != Leader || rf.killed() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.initFollowerState(reply.Term)
		return
	}
	// update nextIndex and matchIndex
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.getLastLogIndex() || rf.killed() {
		return
	}
	logPos := rf.getRelPos(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[logPos].Term
	// cut log
	rf.log = append([]LogEntry{{Term: rf.lastIncludedTerm, Command: nil}},
		rf.log[logPos+1:]...)

	rf.commitIndex = max(index, rf.commitIndex)
	rf.lastApplied = max(index, rf.lastApplied)

	rf.persistWithSnapshot(snapshot)

	if rf.state == Leader {
		rf.heartbeatNow()
	}

	rf.applyCond.Signal()
}

/* ==================== AppendEntries ==================== */

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

	if rf.killed() {
		return
	}
	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.state != Follower {
		// 更新任期并转换为跟随者
		rf.initFollowerState(args.Term)
	}
	rf.resetElectionTimer()

	if args.PrevLogIndex < rf.lastIncludedIndex || args.PrevLogIndex > rf.getLastLogIndex() {
		// 日志不匹配, 且没有可插入点
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// 日志不匹配, 回退以寻找插入点
		conflictTerm := rf.getTerm(args.PrevLogIndex)
		idx := args.PrevLogIndex
		for rf.getRelPos(idx) >= 1 && rf.getTerm(idx-1) == conflictTerm {
			idx--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = idx
		return
	}
	// 日志匹配, 追加新日志
	reply.Success = true
	index := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		if index+i <= rf.getLastLogIndex() {
			// 这里日志一致性证明在论文中已说明
			if rf.getTerm(index+i) != args.Entries[i].Term {
				// 截断冲突日志及其之后的日志, 并追加新日志
				rf.log = rf.log[:rf.getRelPos(index+i)]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			// 直接追加新日志
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader || args.Term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	if nextIndex <= rf.lastIncludedIndex {
		// 需要发送快照
		rf.mu.Unlock()
		go rf.sendInstallSnapshot(server)
		return
	}
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.getTerm(args.PrevLogIndex)
	args.Entries = append([]LogEntry(nil), rf.log[rf.getRelPos(nextIndex):]...)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != Leader || args.Term != rf.currentTerm {
		// 已经不是领导者或任期已变
		return
	}
	if reply.Term > rf.currentTerm {
		// 发现更高任期，转换为跟随者
		rf.initFollowerState(reply.Term)
		rf.resetElectionTimer()
		return
	}
	if reply.Success {
		// 推进nextIndex和matchIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// 更新commitIndex
		lastLogIndex := rf.getLastLogIndex()
		for N := lastLogIndex; N > rf.commitIndex; N-- {
			if N <= rf.lastIncludedIndex {
				// 已包含在快照中
				break
			}
			if rf.getTerm(N) != rf.currentTerm {
				continue
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				rf.applyCond.Signal()
				break
			}
		}
	} else {
		if reply.ConflictTerm == -1 {
			// 对方日志太短或太长
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// 寻找冲突任期的起始索引
			conflictIndex := -1
			for i := rf.getLastLogIndex(); i >= reply.ConflictIndex; i-- {
				if rf.getTerm(i) == reply.ConflictTerm {
					conflictIndex = i
					break
				}
			}
			if conflictIndex == -1 {
				// 本地没有该任期的日志
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				// 回退到该任期的起始位置
				rf.nextIndex[server] = conflictIndex
			}
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if rf.killed() {
			break
		}
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, args)
	}
}

/* ==================== Election and Voting ==================== */

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // 当前任期
	CandidateId  int // 发起选票请求的候选者的标识符
	LastLogIndex int // 候选者的最后日志的索引
	LastLogTerm  int // 候选者的最后日志所属任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // 当前任期，用于候选者更新自己的任期
	VoteGranted bool // 对方是否投票给该候选者
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// 拒绝旧任期的请求
		return
	}
	if args.Term > rf.currentTerm {
		// 更新任期并转换为跟随者
		rf.initFollowerState(args.Term)
		rf.resetElectionTimer()
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	upToDate := func() bool {
		if args.LastLogTerm != lastLogTerm {
			return args.LastLogTerm > lastLogTerm
		} else {
			return args.LastLogIndex >= lastLogIndex
		}
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate() {
		// 投票给该候选者
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
		rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, voteCount *int32) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != Candidate || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		// 发现更高任期，转换为跟随者
		rf.initFollowerState(reply.Term)
		rf.resetElectionTimer()
		return
	}
	if reply.VoteGranted {
		// 计票
		if atomic.AddInt32(voteCount, 1) > int32(len(rf.peers)/2) && rf.state == Candidate && args.Term == rf.currentTerm {
			// 获得多数票, 自身仍为候选者, 且任期未变
			// 有资格成为领导者
			rf.initLeaderState()
			rf.heartbeatNow()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.initCandidateState()
	rf.resetElectionTimer()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var voteCount int32 = 1 // 给自己投票

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, args, &voteCount)
		}
	}
}

/* ==================== Start and Kill ==================== */

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	rf.heartbeatNow()
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	newLog := LogEntry{Command: command, Term: term}
	rf.log = append(rf.log, newLog)
	rf.persist()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	return index, term, true
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
	if !rf.killed() {
		atomic.StoreInt32(&rf.dead, 1)
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/* ==================== Goroutines ==================== */

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.applyCond.L.Lock()
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.applyCond.L.Unlock()
			return
		}

		if rf.lastApplied < rf.lastIncludedIndex {
			// apply wrapped snapshot
			rf.lastApplied = rf.lastIncludedIndex
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.applyCh <- msg
			rf.lastApplied = rf.lastIncludedIndex
			rf.applyCond.L.Unlock()
			continue
		}

		if rf.commitIndex > rf.lastApplied {
			// apply raw log entries
			start := rf.lastApplied + 1
			end := rf.commitIndex
			for i := start; i <= end; i++ {
				if i <= rf.lastIncludedIndex {
					continue
				}
				relPos := rf.getRelPos(i)
				if relPos < 0 || relPos >= len(rf.log) {
					break
				}
				msg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[relPos].Command,
					CommandIndex: i,
				}
				rf.applyCond.L.Unlock()
				rf.applyCh <- msg
				rf.applyCond.L.Lock()
				rf.lastApplied = i
			}
			rf.applyCond.L.Unlock()
		} else {
			rf.applyCond.L.Unlock()
		}
	}
}

func (rf *Raft) electionTimer() {
	randomTimeout := func() time.Duration {
		return time.Duration(500+rand.Intn(400)) * time.Millisecond
	}
	timer := time.NewTimer(randomTimeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			// 选举超时, 通知选举
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				select {
				case rf.electionCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(randomTimeout())
		case <-rf.resetELTimeoutCh:
			// 重置选举定时器
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(randomTimeout())
		case <-rf.killCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	select {
	case rf.resetELTimeoutCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) heartbeatTimer() {
	timeout := func() time.Duration {
		return time.Duration(100 * time.Millisecond)
	}
	timer := time.NewTimer(timeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			// 心跳超时, 发送心跳
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				select {
				case rf.heartbeatCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(timeout())
		case <-rf.resetHBTimeoutCh:
			// 立即心跳
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(0)
		case <-rf.killCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}

}

func (rf *Raft) heartbeatNow() {
	select {
	case rf.resetHBTimeoutCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionCh:
			go rf.startElection() // including leader check
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				go rf.broadcastHeartbeat()
			}
		case <-rf.killCh:
			return
		}
	}
}

/* ==================== Make ==================== */

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
	rf.mu = sync.Mutex{}
	atomic.StoreInt32(&rf.dead, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Command: nil, Term: 0}} // dummy entry
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.electionCh = make(chan struct{}, 1)
	rf.resetELTimeoutCh = make(chan struct{}, 1)
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.resetHBTimeoutCh = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	go rf.applier()
	go rf.electionTimer()
	go rf.heartbeatTimer()
	go rf.ticker()

	return rf
}
