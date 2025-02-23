// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	//-------------self add ------------------//
	peers []uint64
	agreeCount uint64
	randElectionTimeout int
	commitCount uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _, id := range c.peers {
		prs[id] = &Progress{
			Next: 1,
			Match: 0,
		}
	}
	raftLog := newLog(c.Storage)
	hardstate, _, _ := raftLog.storage.InitialState()
	return &Raft{
		Term: 0,
		Vote: hardstate.Vote,
		id : c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		randElectionTimeout: c.ElectionTick,
		RaftLog: raftLog,
		State: StateFollower,
		peers: c.peers,
		heartbeatElapsed: 0,
		electionElapsed: 0,
		votes: make(map[uint64]bool),
		Prs: prs,
		commitCount: 0,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var entries []*pb.Entry
	index := r.Prs[to].Next - r.RaftLog.first
	for k, _ := range r.RaftLog.entries[index:] {
		entries = append(entries, &r.RaftLog.entries[uint64(k) + index])
	}
	logTerm, _ := r.RaftLog.Term(r.Prs[to].Next - 1)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Entries: entries,
		From: r.id,
		To: to,
		Term: r.Term,
		Index: r.Prs[to].Next - 1,
		Commit: r.RaftLog.committed,
		LogTerm: logTerm,
	})
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State{
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if(r.heartbeatElapsed == r.heartbeatTimeout) {
		r.heartbeatElapsed = 0
		for _, to := range r.peers {
			if to != r.id {
				r.msgs = append(r.msgs, pb.Message{
					From: r.id,
					To: to,
					Term: r.Term,
					MsgType: pb.MessageType_MsgHeartbeat,
				})
			}
		}
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed == r.randElectionTimeout {
		r.electionElapsed = 0
		r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.becomeCandidate()
		index := r.RaftLog.LastIndex()
		logTerm, _ := r.RaftLog.Term(index) 
		for _, to := range r.peers {
			if r.id != to {
				r.msgs = append(r.msgs, pb.Message{
					From: r.id,
					To: to,
					Term: r.Term,
					MsgType: pb.MessageType_MsgRequestVote,
					Index: index,
					LogTerm: logTerm,
				})
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if term > r.Term {
		r.Vote = None
	}
	r.Lead = lead
	r.Term = term
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
	r.votes[r.id] = true
	r.Vote = r.id
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{
				Data: nil,
			},
		},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, 0)
		}
		r.followerhandler(m)
	case StateCandidate:
		if r.Term < m.Term {
			r.Term = m.Term
			r.State = StateFollower
			r.Vote = None
			r.Step(m)
			break
		}
		r.candidatehandler(m)
	case StateLeader:
		if r.Term < m.Term {
			r.Term = m.Term
			r.State = StateFollower
			r.Vote = None
			r.Step(m)
			break
		}
		r.leaderhandler(m)
	}
	return nil
}

func (r *Raft) followerhandler(m pb.Message) error {
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for _, id := range r.peers{
			if id == r.id{
				r.Step(pb.Message{
					From: r.id,
					To: r.id,
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: false,
					Term: r.Term,
				})
			} else {
				index := r.RaftLog.LastIndex()
				logTerm, _ := r.RaftLog.Term(index)
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					From: r.id,
					To: id,
					Term: r.Term,
					Index: index,
					LogTerm: logTerm,
				})
			}
		}
	case pb.MessageType_MsgRequestVote:
		var reject bool
		index := r.RaftLog.LastIndex()
		term, _ := r.RaftLog.Term(index)
		
		if r.Vote == None || r.Vote == m.From {
			reject = false
		} else {
			reject = true
		}
		if index != 0 && !reject {
			if m.LogTerm < term {
				reject = true
			} else if m.LogTerm > term {
				reject = false
			} else {
				if m.Index >= index {
					reject = false
				} else {
					reject = true
				}
			}
		}
		if !reject {
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  reject,
		})
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From: r.id,
			To: m.From,
			Term: r.Term,
		})
	}
	return nil
}

func (r *Raft) candidatehandler(m pb.Message) error {
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		// r.Term++
		r.becomeCandidate()
		for _, id := range r.peers{
			if id == r.id{
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: false,
					From: r.id,
					To: r.id,
					Term: r.Term,
				})
			} else {
				index := r.RaftLog.LastIndex()
				logTerm, _ := r.RaftLog.Term(index)
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					From: r.id,
					To: id,
					Term: r.Term,
					Index: index,
					LogTerm: logTerm,
				})
			}
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if(!m.Reject) {
			r.votes[m.From] = true
		} else {
			r.votes[m.From] = false
		}
		var agreeCount, rejectCount uint64
		agreeCount = 0
		rejectCount = 0
		for _, v := range r.votes {
			if v {
				agreeCount++
			} else {
				rejectCount++
			}
		}
		if(agreeCount > uint64(len(r.peers)) / 2) {
			r.becomeLeader()
		} else if (rejectCount > uint64(len(r.peers)) / 2) {
			r.becomeFollower(m.Term, 0)
		}
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
	
	return nil
}

func (r *Raft) leaderhandler(m pb.Message) error {
	switch m.MsgType{
	case pb.MessageType_MsgPropose:
		// 每一个propose请求前将commit计数器归零
		r.commitCount = 0
		// 将消息中的entries添加到本地日志中
		for _, entry := range m.Entries{
			entry.Term = r.Term
			entry.Index = r.RaftLog.LastIndex() + 1
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}

		for _, id := range r.peers {
			if id == r.id{
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgAppendResponse,
					From: r.id,
					To: r.id,
					Term: r.Term,
					Index: r.RaftLog.LastIndex(),
				})
				continue
			}
			r.sendAppend(id)
		}
	case pb.MessageType_MsgAppendResponse:
		logTerm, _ := r.RaftLog.Term(m.Index)
		
		if r.Prs[m.From].Match == m.Index {
			break
		}
		if m.Reject {
			r.Prs[m.From].Next--
			r.sendAppend(m.From)
		} else {
			if  logTerm != r.Term {
				break
			} 
			r.commitCount++
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			if r.commitCount > uint64(len(r.peers) / 2){
				r.RaftLog.committed = m.Index
				// 满足要求后置零，以防之后的响应重复操作
				r.commitCount = 0
				for id, _ := range r.Prs {
					if id == r.id {
						continue
					}
					r.sendAppend(id)
				}
			}
		}
	case pb.MessageType_MsgBeat:
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, 
				pb.Message{
					MsgType: pb.MessageType_MsgHeartbeat,
					From: r.id,
					To: id,
				},
			)
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if r.Prs[m.From].Match != r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
	return nil
}
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	var isReject bool
	var matchIndex uint64

	r.Lead = m.From
	
	if m.Index > r.RaftLog.LastIndex() {
		isReject = true
	} else {
		term, _ :=  r.RaftLog.Term(m.Index)
		if m.LogTerm != term {
			isReject = true
		} else {
			isReject = false
			isChange := false
			index := m.Index
			var key uint64
			for k, v := range m.Entries {
				if (*v).Index > r.RaftLog.LastIndex() {
					isChange = true
					index = r.RaftLog.LastIndex()
					key = uint64(k)
					// r.RaftLog.stabled = index
					break
				}
				t, _ := r.RaftLog.Term((*v).Index)
				if (*v).Term != t {
					isChange = true
					index = (*v).Index - 1
					key = uint64(k)
					// r.RaftLog.stabled = index
					break
				}
			}
			if isChange {
				r.RaftLog.entries = r.RaftLog.entries[:index - r.RaftLog.first + 1]
				for _, v := range m.Entries[key:] {
					r.RaftLog.entries = append(r.RaftLog.entries, *v) 
				}
				r.RaftLog.stabled = min(index, r.RaftLog.stabled)
			}
		}
	}
	matchIndex = r.RaftLog.LastIndex()
	if !isReject {
		r.RaftLog.committed = min(m.Index + uint64(len(m.Entries)), m.Commit)
	}
	r.msgs = append(r.msgs, pb.Message{
		From: m.To,
		To: m.From,
		Term: m.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index: matchIndex,
		Reject: isReject,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
