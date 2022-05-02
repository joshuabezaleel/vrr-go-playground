package vrr

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicaStatus int

// CommitEntry is the data reported by VRR to the commit channel.
// Each commit entry notifies the client that consensus was reached on a received command
// and it can be applied to the client's tate machine.
type CommitEntry struct {
	// Command is the client command being commited.
	Command interface{}

	// Index is the log index at which the client command is commited.
	Index int

	// View is the VRR view number at which the client command is commited.
	View int
}

// clientid
// requestnum
// operation

// viewnum
// requestnum
// response

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

// clientid
// requestnum
// operation

type Request struct {
	ClientID   int
	RequestNum int
	Operation  interface{}
}

func (r *Replica) Submit(request Request) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dlog("Submit received by %v: %v", r.status, request.Operation)
	if r.status == Normal && r.primaryID == r.ID {
		r.opLog = append(r.opLog, request.Operation)
		r.dlog("... log=%v", r.opLog...)
		return true, nil
	}

	return false, errors.New(fmt.Sprintf("Error submitting command %+v", request.ClientID))
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

		if r.status == Recovery {
			r.stateTransfer()
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

func (r *Replica) stateTransfer() {
	for peerID := range r.configuration {
		getStateArgs := GetStateArgs{
			ReplicaID: r.ID,
			ViewNum:   r.viewNum,
			OpNum:     r.opNum,
			CommitNum: r.commitNum,
		}
		var newStateReply NewStateReply

		go func(peerID int) {
			r.dlog("sending <GET-STATE> to %d: %+v", peerID, getStateArgs)
			if err := r.server.Call(peerID, "Replica.StateTransfer", getStateArgs, &newStateReply); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()

				r.viewNum = newStateReply.ViewNum
				r.opNum = newStateReply.OpNum
				r.commitNum = newStateReply.CommitNum
				r.primaryID = newStateReply.PrimaryID
				// TODO: state transfer for r.opLog

				r.dlog("received <NEW-STATE> reply %+v", newStateReply)
			}
		}(peerID)
	}
}

type GetStateArgs struct {
	ViewNum   int
	OpNum     int
	CommitNum int
	ReplicaID int
}

type NewStateReply struct {
	IsReplied bool
	ViewNum   int
	OpNum     int
	CommitNum int
	OpLog     []interface{}
	ReplicaID int
	PrimaryID int
}

func (r *Replica) StateTransfer(args GetStateArgs, reply *NewStateReply) error {
	if r.status != Normal {
		return nil
	}
	r.dlog("GetState: %+v", args)

	reply.IsReplied = true
	reply.ReplicaID = r.ID
	reply.ViewNum = r.viewNum
	reply.OpNum = r.opNum
	reply.CommitNum = r.commitNum
	reply.PrimaryID = r.primaryID

	// TODO: state transfer kalau ketinggalan viewNum aja?
	// kalau misalnya pas viewNumnya sama gimana?
	// yang dicek viewNum aja atau opNum sama commitNum juga?
	//
	// On paper chapter 5.2, TODO for the latter case (recovery when it learned about missing requests in the same viewNum):
	// there are two cases, depending on whether the slow node has learned that it is missing requests in its current view, or has heard about a later view.
	if r.viewNum == args.ViewNum {
		// TODO:
		// opLog is the log between <b>opNum</b> in the incoming GetStateArgs
		// and this replica opNum.
		reply.OpLog = r.opLog
	} else if r.viewNum > args.ViewNum {
		// TODO:
		// opLog is the log between <b>commitNum</b> in the incoming GetStateArgs
		// and this replica opNum.
		reply.OpLog = r.opLog
	} else if r.viewNum < args.ViewNum {
		return nil
	}

	return nil
}

// When the timeout timer at a particular replica expired after not hearing from the primary after some time,
// The replica will initiate view change and send <START-VIEW-CHANGE> messages to ask for quorum to all other replicas
func (r *Replica) initiateViewChange() {
	r.status = ViewChange
	r.doViewChangeCount = 0
	r.viewNum++
	r.viewChangeResetEvent = time.Now()
	r.dlog("TIMEOUT; initiates VIEW-CHANGE: view = %d", r.viewNum)

	// Run another View-Change timer, in case this View-Change is not successful.
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
					if replies*2 >= len(r.configuration)+1 {
						sendDoViewChangeAlready = true

						nextPrimaryID := nextPrimary(r.primaryID, r.configuration)

						args := DoViewChangeArgs{
							ReplicaID:  r.ID,
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
				r.dlog("received <START-VIEW-CHANGE> reply %+v", reply)
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
	ReplicaID  int
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
	if r.status == Dead {
		return nil
	}
	r.dlog("DoViewChange: %+v [currentView = %d]", args, r.viewNum)

	reply.IsReplied = true
	reply.ReplicaID = r.ID

	r.doViewChangeCount++
	r.peerInformation = append(r.peerInformation, backupReplicaInformation{replicaID: args.ReplicaID, viewNum: args.ViewNum, oldViewNum: args.OldViewNum, opNum: args.OpNum, opLog: args.OpLog, commitNum: args.CommitNum})
	r.dlog("DoViewChange messages received: %d", r.doViewChangeCount)

	// The second condition (r.iD != r.primaryID) acts like sendDoViewChangeAlready variable
	// which the designated primary to execute state transfer from another
	// backup replicas and chose itself as primary again
	// if it's already being the primary of the cluster.
	if r.doViewChangeCount*2 > len(r.configuration)+1 && r.ID != r.primaryID {
		// Sort messages received by the next designated primary from all of the backup replicas.
		sort.Slice(r.peerInformation, func(i, j int) bool {
			if r.peerInformation[i].viewNum != r.peerInformation[j].viewNum {
				return r.peerInformation[i].viewNum > r.peerInformation[j].viewNum
			}
			if r.peerInformation[i].oldViewNum != r.peerInformation[j].oldViewNum {
				return r.peerInformation[i].oldViewNum > r.peerInformation[j].oldViewNum
			}
			if r.peerInformation[i].opNum != r.peerInformation[j].opNum {
				return r.peerInformation[i].opNum > r.peerInformation[j].opNum
			}
			if r.peerInformation[i].commitNum != r.peerInformation[j].commitNum {
				return r.peerInformation[i].commitNum > r.peerInformation[j].commitNum
			}
			return true
		})

		mostUpToDateBackupReplica := r.peerInformation[0]
		r.dlog("becomes the new Primary; receiving new state from %d as the most up-to-date backup replicas: %+v", mostUpToDateBackupReplica.replicaID, mostUpToDateBackupReplica)

		// TODO:
		// Uncomment this later
		// oldCommitNum := r.commitNum
		//
		// TODO:
		// For the operation below, upon receiving new state from the most up-to-date backup replicas,
		// Also execute all of the uncommited operations.
		r.viewNum = mostUpToDateBackupReplica.viewNum
		r.oldViewNum = mostUpToDateBackupReplica.oldViewNum
		r.commitNum = mostUpToDateBackupReplica.commitNum
		r.opNum = mostUpToDateBackupReplica.opNum
		r.opLog = mostUpToDateBackupReplica.opLog

		r.blastStartViewAsPrimary()
	}

	r.dlog("... DoViewChange replied: %+v", reply)

	return nil
}

func (r *Replica) blastStartViewAsPrimary() {
	for peerID := range r.configuration {
		args := StartViewArgs{
			PrimaryID: r.ID,
			ViewNum:   r.viewNum,
			CommitNum: r.commitNum,
			OpNum:     r.commitNum,
			OpLog:     r.opLog,
		}

		go func(peerID int) {
			r.dlog("sending START-VIEW to %d; args %+v", peerID, args)
			var reply StartViewReply

			if err := r.server.Call(peerID, "Replica.StartView", args, &reply); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()
			}
		}(peerID)
	}

	r.becomePrimary()
	return
}

type StartViewArgs struct {
	PrimaryID int
	ViewNum   int
	CommitNum int
	OpNum     int
	OpLog     []interface{}
}

type StartViewReply struct {
	IsReplied bool
	ReplicaID int
}

func (r *Replica) StartView(args StartViewArgs, reply *StartViewReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == Dead {
		return nil
	}
	r.dlog("START-VIEW: %+v", args)

	r.opLog = args.OpLog
	r.opNum = args.OpNum
	r.viewNum = args.ViewNum
	r.status = Normal
	r.primaryID = args.PrimaryID

	// TODO: Execute all operation from old commitNum to the new commitNum
	// and send <PREPARE-OK> for all operation in OpLog which haven't been committed yet.

	reply.IsReplied = true
	reply.ReplicaID = r.ID
	r.dlog("StartView reply: %+v", *reply)

	// This is equals to becomeFollower() along with the state transfer of the replica properties'
	// on the few lines above.
	r.doViewChangeCount = 0
	r.viewChangeResetEvent = time.Now()
	go r.runViewChangeTimer()

	return nil
}

func (r *Replica) becomePrimary() {
	r.primaryID = r.ID
	r.status = Normal

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
			r.dlog("sending <COMMIT> as periodic heartbeat to %d; args=%+v", peerID, args)
			var reply CommitReply

			if err := r.server.Call(peerID, "Replica.Commit", args, &reply); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()

				if reply.ViewNum > savedViewNum {
					r.dlog("one of backup replicas with the id of %d got bigger ViewNum, become backup replica", reply.ReplicaID)
					r.status = Normal
					r.primaryID = reply.ReplicaID

					// Become backup replica.
					r.doViewChangeCount = 0
					r.viewChangeResetEvent = time.Now()
					go r.runViewChangeTimer()

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

	if args.ViewNum > r.viewNum {
		r.status = Recovery
	}

	reply.ViewNum = r.viewNum
	r.dlog("COMMIT reply: %+v", *reply)

	return nil
}
