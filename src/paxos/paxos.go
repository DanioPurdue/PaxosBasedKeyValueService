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

import (
	"net"
	"time"
)
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

type GetMinLocalArg struct {}
type GetMinLocalReply struct {
	MinSeq int
}

type LearnerArgs struct{
	SeqNum int
}

type LearnerReply struct{
	SeqNum int
	HasAccepted bool
	AcceptedVal interface{}
}

func (px *Paxos) AcceptorPrepare(proposalArgs *ProposalArgs, proposalReply *ProposalReply ) error{
	// check minimum sequence number
	px.mu.Lock()
	px.maxSeq = maxH(proposalArgs.SeqNum, px.maxSeq)
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
	if proposalArgs.ProposalNum > stateVal.ProposalNum {
		stateVal.ProposalNum = proposalArgs.ProposalNum // update the highest proposal number seen so far
		px.seqLog[proposalArgs.SeqNum] = stateVal //update the proposal number
		proposalReply.IsProposalNumHigh = true
		//fmt.Println("AccepterPrepare | proposal accepted Proposal Number: ", proposalArgs.ProposalNum, " sequence number: ", proposalArgs.SeqNum)
	} else { //got rejected proposal number is not high enough
		proposalReply.IsProposalNumHigh = false
	}
	proposalReply.SeqNum = proposalArgs.SeqNum //proposer check the sequence number
	proposalReply.ProposalNum = stateVal.ProposalNum
	proposalReply.AcceptedNum = stateVal.AcceptedNum
	proposalReply.AcceptedVal = stateVal.AcceptedVal
	//fmt.Println("AccepterPrepare | sequence number (right before return): ", proposalReply.SeqNum)
	px.mu.Unlock()
	return nil
}

func (px *Paxos) AcceptorAccept(acceptArgs *AcceptArgs, acceptReply *AcceptReply ) error{
	px.mu.Lock()
	stateVal, ok := px.seqLog[acceptArgs.SeqNum]
	if ok && acceptArgs.ProposalNum >= stateVal.ProposalNum {
		// If the sequence number is large enough, you accept the proposal and made the update
		stateVal.ProposalNum = acceptArgs.ProposalNum
		stateVal.AcceptedNum = acceptArgs.ProposalNum
		stateVal.AcceptedVal = acceptArgs.ProposalVal
		px.seqLog[acceptArgs.SeqNum] = stateVal
		acceptReply.HasAccepted = true
		go px.learnerPropose(acceptArgs.SeqNum)
	} else {
		acceptReply.HasAccepted = false
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) AcceptorDecided(acceptArgs *AcceptArgs, acceptReply *AcceptReply ) error{
	fate, _ := px.Status(acceptArgs.SeqNum)
	if fate == Pending{
		// If the system has not learned the value
		px.mu.Lock()
		px.learnedVal[acceptArgs.SeqNum] = acceptArgs.ProposalVal
		px.mu.Unlock()
		acceptReply.HasAccepted = true
	} else {
		acceptReply.HasAccepted = false
	}
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
		for notDecided == true && px.isdead() == false {
			acceptCount := 0 // number acceptors that have accepted the values
			highestProposalNum := 0
			highestAcceptedNum := -1
			var highestAcceptedVal interface{} = nil
			for idx, addr := range px.peers { // you can also add a go routine here
				proposeArgs := &ProposalArgs{}
				proposeArgs.SeqNum = seq
				proposeArgs.ProposalVal = proposedVal
				proposeArgs.ProposalNum = proposedNum // learn from the rejection
				proposalReply := &ProposalReply{}
				//proposalReply.SeqNum = -1
				// retry for unreliable network, and the max attempts is 10 times
				if idx == px.me {
					px.AcceptorPrepare(proposeArgs, proposalReply)
				} else {
					for i:= 0; (call(addr, "Paxos.AcceptorPrepare", proposeArgs, proposalReply) == false) && (i < 10) && px.isdead() == false; i++ {}
				}
				// Update the highest accepted proposal number
				if proposalReply.SeqNum != -1 && proposalReply.IsProposalNumHigh == true && proposalReply.AcceptedNum != -1 {
					// Update the highest accepted number, if it has a proposal number
					if highestAcceptedNum < proposalReply.AcceptedNum {
						highestAcceptedVal = proposalReply.AcceptedVal
						highestAcceptedNum = proposalReply.AcceptedNum
					}
				}
				// check sequence number, and check proposal number
				//fmt.Println("proposer side SeqNum: ", proposalReply.SeqNum, " IsProposalNumHigh: ", proposalReply.IsProposalNumHigh)
				if proposalReply.SeqNum != -1 && proposalReply.IsProposalNumHigh == true {
					//fmt.Println("accept count getUpdated")
					acceptCount += 1
				} else if proposalReply.SeqNum != -1 { // under the assumption that RPC made it through
					// learn the rejected highest sequence num
					if highestProposalNum < proposalReply.ProposalNum {
						highestProposalNum = proposalReply.ProposalNum
					}
				}
			}
			/* Accept Phase */
			//fmt.Println("Entering the accept phase... acceptCount: ", acceptCount, " peer size: ", len(px.peers))
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
						//TODO::time.Sleep(1 * time.Second) can be used to avoid conjestion
						for i:= 0; call(addr, "Paxos.AcceptorAccept", acceptArgs, acceptReply) == false && i < 10 && px.isdead() == false; i++ {}
					}
					if acceptReply.HasAccepted == true {
						acceptPhaseCnt += 1
					}
				}
				/* Learn Phase */
				//fmt.Println("Entering the learn phase | acceptPhaseCnt: ", acceptPhaseCnt)
				if acceptPhaseCnt * 2 > len(px.peers) { //the value go accepted by the majority
					//TODO:: have the learner to periodically to learn from the majority
					px.BcastLearnedValue(seq, proposedVal)
					notDecided = false
				}
			} else { //update the proposed num and try it again
				proposedNum = highestProposalNum + 1 //start the next round of proposal
			}
		}
	}(seq, v)
	return
}

func (px *Paxos) BcastLearnedValue(seq int, v interface{}) {
	for idx, addr := range px.peers {
		acceptArgs := &AcceptArgs{}
		acceptArgs.SeqNum = seq
		acceptArgs.ProposalVal = v
		acceptReply := &AcceptReply{}
		if idx == px.me {
			px.AcceptorDecided(acceptArgs, acceptReply)
		} else {
			for i := 0; i < 10 && px.isdead() == false && call(addr, "Paxos.AcceptorDecided", acceptArgs, acceptReply) == false; i++ {}// try to broadcast the value and give the maximum of 10 trial
		}
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	// Your code here
	for i := px.minSeq; i <= seq; i++ {
		delete(px.learnedVal, i)
		delete(px.seqLog, i)
	}
	px.minSeq = maxH(seq + 1, px.minSeq)
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.maxSeq
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

// Helper functions
func minH (a int, b int) int {
	if a < b {return a}
	return b
}

func maxH (a int, b int) int {
	if a > b {return a}
	return b
}
// End of helper functions

func (px *Paxos) Min() int {
	// You code here.
	minVal := px.minSeq
	for idx, addr := range px.peers {
		if idx == px.me {
			continue
		} else {
			getMinLocalArg := &GetMinLocalArg{}
			getMinLocalReply := &GetMinLocalReply{}
			for i:=0; i < 10 && px.isdead() == false; i++{
				isConnected := call(addr,"Paxos.GetMinLocal", getMinLocalArg, getMinLocalReply)
				if isConnected == true {
					minVal = minH(minVal, getMinLocalReply.MinSeq)
					//fmt.Println("Testing Min | minVal: ", minVal)
					break
				}
			}
		}
	}
	if minVal == -1 {
		minVal = 0
	}
	return minVal
}

func (px *Paxos) GetMinLocal(getMinLocalArg *GetMinLocalArg, getMinLocalReply *GetMinLocalReply ) error {
	getMinLocalReply.MinSeq = px.minSeq
	return nil
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
	var fate Fate
	px.mu.Lock()
	if seq < px.minSeq {
		fate = Forgotten
	} else {
		_, ok := px.learnedVal[seq]
		if ok {
			fate = Decided
		} else {
			fate = Pending
		}
	}
	px.mu.Unlock()
	return fate, nil
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
//
// Description: called whenever an acceptor accept something and it does not know whether this is majority
//
func (px *Paxos) learnerPropose(seq int) {
	//sleep a bit to allow for the unreliable network
	//check for unreliable network
	isLearned := false
	for isLearned == false {
		time.Sleep(time.Millisecond)
		px.mu.Lock()
		_, lOk := px.learnedVal[seq]
		px.mu.Unlock()
		if lOk == true {
			// the value has been learned don't bother as well
			isLearned = true
		} else {
			//now let's learn the value
			stats := make(map[interface{}]int)
			for idx, addr := range px.peers {
				learnerArgs := &LearnerArgs{SeqNum: seq}
				learnerReply := &LearnerReply{}
				if idx == px.me {
					px.LearnerLearn(learnerArgs, learnerReply)
				} else {
					for i := 0; i < 10 && px.isdead() == false && (call(addr, "Paxos.LearnerLearn", learnerArgs, learnerReply) == false); i++ {}
				}
				if learnerReply.SeqNum == -1 || learnerReply.HasAccepted == false {
					continue
				}
				_, ok := stats[learnerReply.AcceptedVal]
				if ok == false {
					stats[learnerReply.AcceptedVal] = 1
				} else {
					stats[learnerReply.AcceptedVal] += 1
				}
			}
			for val, cnt := range stats {
				if cnt * 2 <= len(px.peers) {
					continue
				}
				// there is a majority and update the part.
				px.mu.Lock()
				px.learnedVal[seq] = val
				px.mu.Unlock()
				isLearned = true
				break
			}
		}
	}
}

func (px *Paxos) LearnerLearn(learnerArgs *LearnerArgs, learnerReply *LearnerReply) error {
	px.mu.Lock()
	stateVal, ok := px.seqLog[learnerArgs.SeqNum]
	px.mu.Unlock()
	learnerReply.SeqNum = learnerArgs.SeqNum
	// If the sequence number does not exist or there is accepted number
	if ok == false || stateVal.AcceptedNum == -1 {
		learnerReply.HasAccepted = false
	} else {
		learnerReply.HasAccepted = true
		learnerReply.AcceptedVal = stateVal.AcceptedVal
	}
	return nil
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
		// Learner
		// End of Learner
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
