package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	seqLog map[int]LogVal
	learnedVal map[int]interface{}
	minSeq int
	maxSeq int
}

type LogVal struct {
	ProposalNum int // highest proposal have seen
	AcceptedNum int // accepted proposal number
	AcceptedVal interface{} // accepted value
}

type ProposalArgs struct {
	SeqNum int
	ProposalNum int
	ProposalVal interface{}
}

type ProposalReply struct {
	SeqNum int
	ProposalNum int
	IsProposalNumHigh bool
	AcceptedNum int
	AcceptedVal interface{}
}

type AcceptArgs struct {
	SeqNum int
	ProposalNum int
	ProposalVal interface{}
}

type AcceptReply struct {
	HasAccepted bool
}

func (px *Paxos) Propose(proposalArgs *ProposalArgs, proposalReply *ProposalReply, acceptorAddr string) bool {
	// You need to separate the difference between unreliable connection and rejection
	ok := call(acceptorAddr, "Paxos.AcceptorPrepare", proposalArgs, proposalReply)
	return ok
}

func (px *Paxos) AcceptorPrepare(proposalArgs *ProposalArgs, proposalReply *ProposalReply ) error{
	// check minimum sequence number
	px.mu.Lock()
	stateVal, ok := px.seqLog[proposalArgs.SeqNum]
	if ok == false && proposalArgs.SeqNum >= px.minSeq {
		// the instance does not exist
		stateVal = LogVal{ProposalNum: -1, AcceptedNum: -1, AcceptedVal: -1}
	} else if ok == false && proposalArgs.SeqNum < px.minSeq {
		// return false the sequence number you required is below the threshold
		proposalReply.SeqNum = -1
		px.mu.Unlock()
		return nil
	}
	if stateVal.ProposalNum > proposalArgs.ProposalNum {
		stateVal.ProposalNum = proposalArgs.ProposalNum
		px.seqLog[proposalArgs.SeqNum] = stateVal //update the proposal number
		proposalReply.IsProposalNumHigh = true
	} else { //got rejected proposal number is not high enough
		proposalReply.IsProposalNumHigh = false
	}
	proposalReply.SeqNum = proposalArgs.SeqNum
	//proposer use this value to check whether is has been accepted
	proposalReply.ProposalNum = stateVal.ProposalNum
	proposalReply.AcceptedNum = stateVal.AcceptedNum
	proposalReply.AcceptedVal = stateVal.AcceptedVal
	px.mu.Unlock()
	return nil
}

func (px *Paxos) AcceptorAccept(acceptArgs *AcceptArgs, acceptReply *AcceptReply ) error{
	px.mu.Lock()
	stateVal, ok := px.seqLog[acceptArgs.SeqNum]
	if ok && acceptArgs.ProposalNum > stateVal.ProposalNum {
		// If the sequence number is large enough, you accept the proposal and made the update
		stateVal.ProposalNum = acceptArgs.ProposalNum
		stateVal.AcceptedNum = acceptArgs.ProposalNum
		stateVal.AcceptedVal = acceptArgs.ProposalVal
		px.seqLog[acceptArgs.SeqNum] = stateVal
		acceptReply.HasAccepted = true
	} else {
		acceptReply.HasAccepted = false
	}
	px.mu.Unlock()
	return nil
}
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	// probably need to add a go routine here
	// make sure to add appropriate lock
	go func(seq int, v interface{}) {
		notDecided := true
		proposedVal := v
		proposedNum := 0
		for notDecided == true {
			acceptCount := 0
			highestProposalNum := 0
			highestAcceptedNum := -1
			var highestAcceptedVal interface{} = nil
			for idx, addr := range px.peers { // you can also add a go routine here
				proposeArgs := &ProposalArgs{}
				proposeArgs.SeqNum = seq
				proposeArgs.ProposalVal = proposedVal
				proposeArgs.ProposalNum = proposedNum // learn from the rejection
				proposeRely := &ProposalReply{SeqNum: -1}
				// retry for unreliable network, and the max attempts is 10 times
				if idx == px.me {
					px.AcceptorPrepare(proposeArgs, proposeRely)
				} else {
					for i:= 0; call(addr, "Paxos.AcceptorPrepare", proposeArgs, proposeRely) == false && i < 10; i++ {}
				}
				// update the highest accepted number
				if proposeRely.SeqNum != -1 && proposeRely.IsProposalNumHigh == true && proposeRely.AcceptedNum != -1 {
					//update a highest proposal number
					if highestAcceptedNum < proposeRely.AcceptedNum {
						highestAcceptedVal = proposeRely.AcceptedVal
						highestAcceptedNum = proposeRely.AcceptedNum
					}
				}
				// check sequence number, and check proposal number
				if proposeRely.SeqNum != -1 && proposeRely.IsProposalNumHigh == true {
					acceptCount += 1
				} else if proposeRely.SeqNum != -1 {
					//learn the rejected highest sequence num
					if highestProposalNum < proposeRely.ProposalNum {
						highestProposalNum = proposeRely.ProposalNum
					}
				}
			}
			if acceptCount * 2 > len(px.peers) {
				//accepted by the majority
				//proceed to the accept phase
				if highestAcceptedVal != nil {
					//the proposed val is either your value or the highest proposed value
					proposedVal = highestAcceptedVal
				}
				acceptPhaseCnt := 0
				for idx, addr := range px.peers {
					acceptArgs := &AcceptArgs{}
					acceptReply := &AcceptReply{}
					acceptArgs.ProposalVal = proposedVal
					acceptArgs.SeqNum = seq
					acceptArgs.ProposalNum = proposedNum
					if idx == px.me {
						px.AcceptorAccept(acceptArgs, acceptReply)
					} else {
						for i:= 0; call(addr, "Paxos.AcceptorAccept", acceptArgs, acceptReply) == false && i < 10; i++ {}
					}
					if acceptReply.HasAccepted == true {
						acceptPhaseCnt += 1
					}
				}
				if acceptPhaseCnt * 2 < len(px.peers) { //the value go accepted by the majority
					px.mu.Lock()
					//this is self learned it has been accepted by the majority
					px.learnedVal[seq] = proposedVal
					px.mu.Unlock()
					notDecided = false
				}
			} else { //update the proposed num and try it again
				proposedNum = highestProposalNum + 1 //start the next round of proposal
			}
		}
	}(seq, v)
	return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	// Danio: so no rpc in this case
	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.seqLog = make(map[int]LogVal)
	px.learnedVal = make(map[int]interface{})
	px.minSeq = -1
	px.maxSeq = -1
	// end of my initialization
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
