package kvpaxos

import (
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
//import "../paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	IsPut bool
	IsAppend bool
	IsGet bool
	IsNoOp bool
	OpKey string //this is shared by all three types of operations
	OpVal string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seq		   int
	str2str    map[string]string
	reqHist    map[int64]bool
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	// Check the request code to see whether it has been applied or not
	// If it has not applied then start a new sequence
	// Your code here.
	//Duplicate Client Request: If you submit the request to the same machine, this will prevent you from
	//Issuing the request at the first place.
	kv.mu.Lock()
	if kv.reqHist[args.ReqCode] == true {
		// Operation has been applied
		reply.Err = ""
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	// This server got the operation this request is received for the first time.
	kv.reqHist[args.ReqCode] = true
	// Prepare the operation
	var op Op
	op = Op{IsPut: false, IsAppend: false, IsGet: true, IsNoOp: false, OpKey: args.Key, OpVal:""}
	isSettled := false
	to := 10 * time.Millisecond
	for isSettled == false {
		kv.mu.Lock()
		seqNum := kv.seq
		kv.px.Start(seqNum, op)
		//update the sequence number
		kv.seq += 1
		kv.mu.Unlock()

		//check whether the server is deaded
		if kv.isdead()== true {
			reply.Err = "server is down"
			return nil
		}
		for {
			status, learnedVal := kv.px.Status(seqNum)
			if status == paxos.Decided {
				learnedOp, _ := learnedVal.(Op)
				if learnedOp.OpKey != op.OpKey {
					// operation key avoid duplicated action
					// The proposed sequence number proposed by you has not been accepted
					// increase the sequence number
					break
				}
				//check all the previous ops
				minSeq := kv.px.Min()
				isReady := false
				for isReady == false {
					isReady = true
					//check whether previous status has been accepted
					for i := minSeq ; i < seqNum; i++ {
						status, _ := kv.px.Status(i)
						if status != paxos.Decided {
							isReady = false
						}
					}
				}
				kv.mu.Lock()
				kv.px.Done(seqNum) //free the unused memory
				reply.Value = kv.str2str[args.Key]
				kv.mu.Unlock()
				reply.Err = ""
				return nil
			}
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//Duplicate Client Request: If you submit the request to the same machine, this will prevent you from
	//Issuing the request at the first place.
	kv.mu.Lock()
	if kv.reqHist[args.ReqCode] == true {
		// Operation has been applied
		reply.Err = ""
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	// This server got the operation this request is received for the first time.
	kv.reqHist[args.ReqCode] = true
	// Prepare the operation
	var op Op
	if args.Op == "Put" {
		op = Op{IsPut: true, IsAppend: false, IsGet: false, IsNoOp: false, OpKey: args.Key, OpVal: args.Value}
	} else if args.Op == "Append" {
		op = Op{IsPut: true, IsAppend: false, IsGet: false, IsNoOp: false, OpKey: args.Key, OpVal: args.Value}
	}

	isSettled := false
	to := 10 * time.Millisecond
	for isSettled == false {
		kv.mu.Lock()
		seqNum := kv.seq
		kv.px.Start(seqNum, op)
		//update the sequence number
		kv.seq += 1
		kv.mu.Unlock()


		for {
			status, learnedVal := kv.px.Status(seqNum)
			if status == paxos.Decided {
				learnedOp, _ := learnedVal.(Op)
				if learnedOp.OpKey != op.OpKey { //operation key avoid duplicated action
					// The proposed sequence number proposed by you has not been accepted
					// increase the sequence number
					break
				}
				//check all the previous ops
				minSeq := kv.px.Min()
				isReady := false
				for isReady == false {
					isReady = true
					//check whether previous status has been accepted
					for i := minSeq ; i < seqNum; i++ {
						status, _ := kv.px.Status(i)
						if status != paxos.Decided {
							isReady = false
						}
					}
				}
				kv.mu.Lock()
				if op.IsPut {
					kv.str2str[op.OpKey] = op.OpVal
				} else if op.IsAppend {
					kv.str2str[op.OpKey] += kv.str2str[op.OpKey] + op.OpVal
				}
				kv.px.Done(seqNum)
				kv.mu.Unlock()
				reply.Err = ""
				return nil
			}
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.str2str = make(map[string]string)
	kv.reqHist = make(map[int64]bool)
	kv.seq = 0
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
