package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"

import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	serverIdx int //record the server that you would like to contact
	downCnt int
	serverUp map[int]bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.serverIdx = 0
	ck.downCnt = 0
	ck.serverUp = make(map[int]bool)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{}
	reply := &GetReply{}
	args.Key = key
	args.ReqCode = nrand()
	for {
		ok := call(ck.servers[ck.serverIdx], "KVPaxos.Get", args, reply)
		if ok == false || reply.Err != "" {
			fmt.Println("Get | Switching server old addr: ", ck.servers[ck.serverIdx], "serverIdx: ", ck.serverIdx, "Server length: ", len(ck.servers))
			if ok == false || reply.Err == "server is down" {
				ck.serverUp[ck.serverIdx] = false
				ck.serverIdx = (ck.serverIdx + 1) % len(ck.servers)
				ck.downCnt += 1
				if ck.downCnt >= len(ck.servers) {
					return ""
				}
			}

		} else {
			break
		}
	}
	fmt.Println("client | Done get: ", key, " ", args.ReqCode)
	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{}
	reply := &PutAppendReply{}
	args.Op = op
	args.Key = key
	args.Value = value
	args.ReqCode = nrand()
	for {
		ok := call(ck.servers[ck.serverIdx], "KVPaxos.PutAppend", args, reply)
		if ok == false || reply.Err != "" {
			fmt.Println("PutAppend | Switching server old addr: ", ck.servers[ck.serverIdx], "serverIdx: ", ck.serverIdx)
			if ok == false || reply.Err == "server is down"{
				ck.serverUp[ck.serverIdx] = false
				ck.serverIdx = (ck.serverIdx + 1) % len(ck.servers)
				ck.downCnt += 1
				if ck.downCnt >= len(ck.servers) {
					return
				}
			}
		} else {
			break
		}
	}
	fmt.Println("client | Done Put Append: ", key, " ", args.ReqCode)
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
