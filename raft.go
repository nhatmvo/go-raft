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

func New(id int, peerIds []int, server *Server)

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

}

func (r *Raft) becomeLeader() {

}
