package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

const DebugRaft = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu sync.Mutex

	id      int
	peerIds []int

	state RaftState

	server *Server

	currentTerm int
	votedFor    int
	logEntries  []LogEntry

	electionResetTime time.Time
}

func New(id int, peerIds []int, server *Server) {

}

func (r *Raft) dLog(format string, args ...interface{}) {
	if DebugRaft > 0 {
		format = fmt.Sprintf("Node [%d]: ", r.id) + format
		log.Printf(format, args...)
	}
}

func (r *Raft) runElectionTimer() {
	timeoutDuration := r.setElectionTimeout()
	r.mu.Lock()
	termStarted := r.currentTerm
	r.mu.Unlock()
	r.dLog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		r.mu.Lock()
		if r.state != Candidate && r.state != Follower {
			r.dLog("in election state=%s, bailing out", r.state)
			r.mu.Unlock()
			return
		}

		if termStarted != r.currentTerm {
			r.dLog("in election timer term changed from %d to %d, bailing out", termStarted, r.currentTerm)
			r.mu.Unlock()
			return
		}

		if elapse := time.Since(r.electionResetTime); elapse > timeoutDuration {
			r.startElection()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

func (r *Raft) setElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResult struct {
	Term        int
	VoteGranted bool
}

func (r *Raft) startElection() {
	r.state = Candidate
	r.currentTerm += 1
	savedCurrentTerm := r.currentTerm
	r.electionResetTime = time.Now()
	r.votedFor = r.id
	r.dLog("becomes Candidate (term=%d); log=%v", savedCurrentTerm, r.logEntries)

	voteReceived := 1

	for _, peerId := range r.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: r.id,
			}

			var response RequestVoteResult
			r.dLog("sending RequestVote to %d: %v+", peerId, args)
			if err := r.server.Call(peerId, "Raft.RequestVote", args, &response); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.dLog("received RequestVoteResult %+v", response)

				if r.state != Candidate {
					r.dLog("while waiting for response, state=%v", r.state)
					return
				}

				if response.Term > savedCurrentTerm {
					r.dLog("current term is out of date with RequestVoteResult")
					r.becomeFollower(response.Term)
					return
				} else if response.Term == savedCurrentTerm {
					if response.VoteGranted {
						voteReceived++
						if voteReceived*2 > len(r.peerIds)+1 {
							r.dLog("win election (term=%d) with %d votes", savedCurrentTerm, voteReceived)
							r.becomeLeader()
							return
						}
					}
				}
			}

		}(peerId)
	}
}

func (r *Raft) becomeFollower(currentTerm int) {
	r.dLog("becomes Follower (term=%d). logs=%+v", currentTerm, r.logEntries)
	r.state = Follower
	r.currentTerm = currentTerm
	r.votedFor = -1
	r.electionResetTime = time.Now()

	go r.runElectionTimer()
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.dLog("becomes Leader (term=%d). Logs: (%+v)", r.currentTerm, r.logEntries)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			r.sendHeartbeats()
			<-ticker.C

			r.mu.Lock()
			if r.state != Leader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []LogEntry

	LeaderCommit int
}

type AppendEntriesResult struct {
	Term    int
	Success bool
}

func (r *Raft) sendHeartbeats() {
	r.mu.Lock()
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	for _, peerId := range r.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: r.id,
		}
		go func(peerId int) {
			r.dLog("sending AppendEntries to %d, args=%+v", peerId, args)
			var response AppendEntriesResult
			if err := r.server.Call(peerId, "Raft.AppendEntries", args, &response); err == nil {
				r.mu.Lock()
				defer r.mu.Unlock()
				if response.Term > savedCurrentTerm {
					r.dLog("term out of heartbeat reply (from server=%d)", peerId)
					r.becomeFollower(response.Term)
					return
				}
			}
		}(peerId)
	}
}

func (r *Raft) RequestVote(args RequestVoteArgs, response *RequestVoteResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Shutdown {
		return nil
	}
	r.dLog("RequestVote: %+v (currentTerm=%d, votedFor=%d)", args, r.currentTerm, r.votedFor)

	if args.Term > r.currentTerm {
		r.dLog("current term out of date with RequestVote")
		r.becomeFollower(args.Term)
	}

	if args.Term == r.currentTerm && (r.votedFor == -1 || r.votedFor == args.CandidateId) {
		response.VoteGranted = true
		r.votedFor = args.CandidateId
		r.electionResetTime = time.Now()
	} else {
		response.VoteGranted = false
	}
	response.Term = r.currentTerm
	r.dLog("RequestVote response: %+v", *response)
	return nil
}

func (r *Raft) AppendEntries(args AppendEntriesArgs, response *AppendEntriesResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Shutdown {
		return nil
	}
	r.dLog("AppendEntries: %+v (currentTerm=%d)", args, r.currentTerm)

	if args.Term > r.currentTerm {
		r.dLog("current term out of date with AppendEntries")
		r.becomeFollower(args.Term)
	}

	response.Success = false
	if args.Term == r.currentTerm {
		if r.state != Follower {
			r.becomeFollower(args.Term)
		}
		r.electionResetTime = time.Now()
		response.Success = true
	}
	response.Term = r.currentTerm
	r.dLog("AppendEntries response: %+v", *response)
	return nil
}
