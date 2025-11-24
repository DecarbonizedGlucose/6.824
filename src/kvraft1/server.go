package kvraft

import (
	"sync"
	"sync/atomic"

	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type kvEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu   sync.Mutex
	data map[string]kvEntry
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch t := req.(type) {
	case *rpc.GetArgs:
		return kv.doGet(t)
	case rpc.GetArgs:
		return kv.doGet(&t)
	case *rpc.PutArgs:
		return kv.doPut(t)
	case rpc.PutArgs:
		return kv.doPut(&t)
	default:
		panic("未知的类型")
	}
}

func (kv *KVServer) doGet(args *rpc.GetArgs) rpc.GetReply {
	if kv.killed() {
		return rpc.GetReply{Err: rpc.ErrWrongLeader}
	}
	if ent, ok := kv.data[args.Key]; ok {
		return rpc.GetReply{
			Value:   ent.Value,
			Version: ent.Version,
			Err:     rpc.OK,
		}
	}
	return rpc.GetReply{Err: rpc.ErrNoKey}
}

func (kv *KVServer) doPut(args *rpc.PutArgs) rpc.PutReply {
	if kv.killed() {
		return rpc.PutReply{Err: rpc.ErrWrongLeader}
	}
	if env, ok := kv.data[args.Key]; ok {
		if env.Version == args.Version {
			env.Value = args.Value
			env.Version++
			kv.data[args.Key] = env
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	if args.Version == 0 {
		kv.data[args.Key] = kvEntry{Value: args.Value, Version: 1}
		return rpc.PutReply{Err: rpc.OK}
	}
	return rpc.PutReply{Err: rpc.ErrNoKey}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.killed() {
		return nil
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.data) != nil {
		panic("Failed to encode KVServer snapshot")
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if kv.killed() {
		return
	}
	if kv.data == nil {
		kv.data = make(map[string]kvEntry)
	}
	if len(data) == 0 {
		kv.data = make(map[string]kvEntry)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]kvEntry
	if d.Decode(&kvData) != nil {
		panic("Failed to decode KVServer snapshot")
	}

	kv.mu.Lock()
	kv.data = kvData
	kv.mu.Unlock()

}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, ret := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	getReply := ret.(rpc.GetReply)
	reply.Value = getReply.Value
	reply.Version = getReply.Version
	reply.Err = getReply.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, ret := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	putReply := ret.(rpc.PutReply)
	reply.Err = putReply.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(kvEntry{})

	kv := &KVServer{
		me:   me,
		mu:   sync.Mutex{},
		data: make(map[string]kvEntry),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
