package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
	id   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.name = l
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	lk.id = kvtest.RandValue(8)
	for {
		old, v, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey || (err == rpc.OK && old == "") {
			err = lk.ck.Put(lk.name, lk.id, v)
			if err == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	owner, v, err := lk.ck.Get(lk.name)
	if err == rpc.OK && owner == lk.id {
		_ = lk.ck.Put(lk.name, "", v)
	}
}
