package lock

import (
	"time"

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
	lockKey  string
	clientId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.clientId = kvtest.RandValue(8)
	lk.lockKey = l
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		ownerId, version, err := lk.ck.Get((lk.lockKey))
		var putErr rpc.Err

		if err == rpc.ErrNoKey {
			putErr = lk.ck.Put(lk.lockKey, lk.clientId, 0)
		} else if err == rpc.OK {
			if ownerId == "" {
				putErr = lk.ck.Put(lk.lockKey, lk.clientId, version)
			}
		} else {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if putErr == rpc.OK || putErr == rpc.ErrMaybe {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		ownerId, version, err := lk.ck.Get(lk.lockKey)

		if err == rpc.ErrNoKey {
			return
		}

		if err != rpc.OK {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if ownerId != lk.clientId {
			return
		}

		putErr := lk.ck.Put(lk.lockKey, "", version)

		if putErr == rpc.OK || putErr == rpc.ErrMaybe {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}
