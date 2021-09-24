package vrr

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicaStatus int

const (
	Normal ReplicaStatus = iota
	Recovery
	ViewChange
	Transitioning
	Dead
	DoViewChange
	StartView
)

func (rs ReplicaStatus) String() string {
	switch rs {
	case Normal:
		return "Normal"
	case Recovery:
		return "Recovery"
	case ViewChange:
		return "View-Change"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type Replica struct {
	mu sync.Mutex

	ID int

	configuration map[int]string

	server *Server

	oldViewNum int
	viewNum    int
	commitNum  int
	opNum      int
	opLog      []interface{}
	primaryID  int

	// Temporary data used by next designated primary to sort out data
	// when it received various <DO-VIEW-CHANGE> message from other
	// backup replicas.
	doViewChangeCount int
	peerInformation   []backupReplicaInformation

	status               ReplicaStatus
	viewChangeResetEvent time.Time
}

type backupReplicaInformation struct {
	replicaID  int
	viewNum    int
	oldViewNum int
	opNum      int
	opLog      []interface{}
	commitNum  int
}

func NewReplica(ID int, configuration map[int]string, server *Server, ready <-chan interface{}) *Replica {
	replica := new(Replica)
	replica.ID = ID
	replica.configuration = configuration
	replica.server = server
	replica.oldViewNum = -1
	replica.doViewChangeCount = 0
	replica.peerInformation = make([]backupReplicaInformation, 0)

	replica.status = Normal

	go func() {
		<-ready
		replica.mu.Lock()
		replica.viewChangeResetEvent = time.Now()
		replica.mu.Unlock()

		// Replica [0] has been designated as primary from the beginning.
		if replica.ID != replica.primaryID {
			replica.runViewChangeTimer()
		} else {
			replica.becomePrimary()
		}
	}()

	return replica
}

func (r *Replica) Report() (id int, viewNum int, primaryID int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.ID, r.viewNum, r.primaryID
}

func (r *Replica) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.status = Dead
	r.dlog("becomes Dead")
}

func (r *Replica) dlog(format string, args ...interface{}) {
	format = fmt.Sprintf("[%d] ", r.ID) + format
	log.Printf(format, args...)
}

func (r *Replica) runViewChangeTimer() {
	timeoutDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
	r.mu.Lock()
	viewStarted := r.viewNum
	r.mu.Unlock()
	r.dlog("view change timer started (%v), view=%d", timeoutDuration, viewStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		r.mu.Lock()

		if r.status == ViewChange {
			r.blastStartViewChange()
			r.mu.Unlock()
			return
		}

		if elapsed := time.Since(r.viewChangeResetEvent); elapsed >= timeoutDuration {
			r.initiateViewChange()
			r.mu.Unlock()
			return
		}

		r.mu.Unlock()
	}
}

// When the timeout timer at a particular replica expired after not hearing from the primary after some time,
// The replica will initiate view change and send <START-VIEW-CHANGE> messages to ask for quorum to all other replicas
func (r *Replica) initiateViewChange() {
	r.status = ViewChange
	r.doViewChangeCount = 0
	r.viewNum++
	r.viewChangeResetEvent = time.Now()
	r.dlog("TIMEOUT; initiates VIEW-CHANGE: view = %d", r.viewNum)

	go r.runViewChangeTimer()
}

func nextPrimary(primaryID int, config map[int]string) int {
	nextPrimaryID := primaryID + 1
	if nextPrimaryID == len(config)+1 {
		nextPrimaryID = 0
	}

	return nextPrimaryID
}

func (r *Replica) blastStartViewChange() {
	var repliesReceived int32 = 1
	savedViewNum := r.viewNum
	savedOldViewNum := r.oldViewNum
	savedCommitNum := r.commitNum
	savedOpNum := r.opNum
	savedOpLog := r.opLog
	// This variable is used as a marker if the replica already send <DO-VIEW-CHANGE> to
	// the next designated primary after the quorum acknowledged and agreed on View-Change.
	// This is to prevent sending <DO-VIEW-CHANGE> multiple times by this same replica to the new primary.
	var sendDoViewChangeAlready bool = false

	for peerID := range r.configuration {
		args := StartViewChangeArgs{
			ViewNum:   savedViewNum,
			ReplicaID: r.ID,
		}
		var reply StartViewChangeReply

		go func(peerID int) {
			r.dlog("sending <START-VIEW-CHANGE> to %d: %+v", peerID, args)
			if err := r.server.Call(peerID, "Replica.StartViewChange", args, &reply); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()

				if reply.IsReplied && !sendDoViewChangeAlready {
					replies := int(atomic.AddInt32(&repliesReceived, 1))
					if replies*2 > len(r.configuration)+1 {
						sendDoViewChangeAlready = true

						nextPrimaryID := nextPrimary(r.primaryID, r.configuration)

						args := DoViewChangeArgs{
							ViewNum:    savedViewNum,
							OldViewNum: savedOldViewNum,
							CommitNum:  savedCommitNum,
							OpNum:      savedOpNum,
							OpLog:      savedOpLog,
						}
						var reply DoViewChangeReply

						r.dlog("acknowledge that quorum agrees on a View Change, sending <DO-VIEW-CHANGE> to the new designated primary %d, %+v", nextPrimaryID, args)
						if err := r.server.Call(nextPrimaryID, "Replica.DoViewChange", args, &reply); err == nil {
							return
						} else {
							r.dlog("Error Replica.DoViewChange = %v", err.Error())
						}

						return
					}
				}
				r.dlog("received <START-VIEW-CHANGE reply %+v", reply)
			}
		}(peerID)
	}
}

type StartViewChangeArgs struct {
	ViewNum   int
	ReplicaID int
}

type StartViewChangeReply struct {
	IsReplied bool
	ReplicaID int
}

func (r *Replica) StartViewChange(args StartViewChangeArgs, reply *StartViewChangeReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == Dead {
		return nil
	}
	r.dlog("START-VIEW-CHANGE: %+v [currentView = %d]", args, r.viewNum)

	reply.IsReplied = true
	reply.ReplicaID = r.ID

	if args.ViewNum > r.viewNum {
		r.status = ViewChange
		r.oldViewNum = r.viewNum
		r.viewNum = args.ViewNum
		r.viewChangeResetEvent = time.Now()
	}

	r.dlog("START-VIEW-CHANGE replied: %+v", *reply)
	return nil
}

type DoViewChangeArgs struct {
	ViewNum    int
	OldViewNum int
	CommitNum  int
	OpNum      int
	OpLog      []interface{}
}

type DoViewChangeReply struct {
	IsReplied bool
	ReplicaID int
}

func (r *Replica) DoViewChange(args DoViewChangeArgs, reply *DoViewChangeReply) error {
	// r.mu.Lock()
	// defer r.mu.Unlock()

	if r.status == Dead {
		return nil
	}
	r.dlog("DoViewChange: %+v [currentView = %d]", args, r.viewNum)

	reply.IsReplied = true
	reply.ReplicaID = r.ID

	r.doViewChangeCount++
	r.dlog("DoViewChange messages received: %d", r.doViewChangeCount)

	r.dlog("... DoViewChange replied: %+v", reply)

	return nil
}

func (r *Replica) becomePrimary() {
	r.mu.Lock()
	r.status = Normal
	r.mu.Unlock()

	go func() {

		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			r.sendPrimaryPeriodicCommits()
			<-ticker.C

			r.mu.Lock()
			if r.primaryID != r.ID {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}()
}

func (r *Replica) sendPrimaryPeriodicCommits() {
	r.mu.Lock()
	savedViewNum := r.viewNum
	savedCommitNum := r.commitNum
	r.mu.Unlock()

	for peerID := range r.configuration {
		args := CommitArgs{
			ViewNum:   savedViewNum,
			CommitNum: savedCommitNum,
			PrimaryID: r.ID,
		}

		go func(peerID int) {
			r.dlog("sending <COMMIT> as period heartbeat to %d; args=%+v", peerID, args)
			var reply CommitReply

			if err := r.server.Call(peerID, "Replica.Commit", args, &reply); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()

				if reply.ViewNum > savedViewNum {
					r.dlog("one of backup replicas got bigger ViewNum, become backup replica")
					// r.becomeBackupReplica(reply.ViewNum)
					return
				}
			}
		}(peerID)
	}
}

type CommitArgs struct {
	ViewNum   int
	CommitNum int
	PrimaryID int
}

type CommitReply struct {
	IsReplied bool
	ReplicaID int
	ViewNum   int
	Status    string
}

func (r *Replica) Commit(args CommitArgs, reply *CommitReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == Dead {
		return nil
	}
	r.dlog("COMMIT: %+v", args)

	if args.ViewNum == r.viewNum {
		r.viewChangeResetEvent = time.Now()
		reply.IsReplied = true
		reply.ReplicaID = r.ID
		reply.Status = r.status.String()
	}

	reply.ViewNum = r.viewNum
	r.dlog("COMMIT reply: %+v", *reply)

	return nil
}
