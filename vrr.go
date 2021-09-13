package vrr

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
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

		if elapsed := time.Since(r.viewChangeResetEvent); elapsed >= timeoutDuration {
			r.dlog("TIMEOUT!")
			r.mu.Unlock()
			return
		}

		r.mu.Unlock()
	}
}

func (r *Replica) becomePrimary() {
	r.mu.Lock()
	r.status = Normal
	r.mu.Unlock()

	go func() {

		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			r.sendPrimaryPeriodCommits()
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

func (r *Replica) sendPrimaryPeriodCommits() {
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
			r.dlog("sending periodic <COMMIT> as heartbeat to %d; args=%+v", peerID, args)
			var reply CommitReply

			err := r.server.Call(peerID, "Replica.Commit", args, &reply)
			if err != nil {
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
}

func (r *Replica) Commit(args CommitArgs, reply *CommitReply) error {
	r.dlog("JOBEL")
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
	}

	reply.ViewNum = r.viewNum
	r.dlog("COMMIT reply: %+v", *reply)

	return nil
}

// type HelloArgs struct {
// 	ID int
// }

// type HelloReply struct {
// 	ID int
// }

// func (r *Replica) Hello(args HelloArgs, reply *HelloReply) error {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	if r.status == Dead {
// 		return nil
// 	}
// 	r.dlog("received the greetings from %d! :)", args.ID)
// 	reply.ID = r.ID
// 	return nil
// }
