// Copyright 2018 The https://github.com/junwen12221 Authors
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

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/***
 * cjw
 * @finished
 */
@Builder
@Data
public class Raft {

    // None is a placeholder node ID used when there is no leader.
    public static final long None = 0L;
    public static final long noLimit = Long.MAX_VALUE;

    long id;
    long term = None;
    long vote = None;

    List<ReadOnly.ReadState> readStates = new ArrayList<>();

    long raftState = None;

    // the log
    RaftLog raftLog;

    int maxInflight;
    long maxMsgSize;

    Map<Long, Process> prs;
    Map<Long, Process> leaderPrs;

    long[] matchBuf;

    // Possible values for StateType.
    enum StateType {
        StateFollower,
        StateCandidate,
        StateLeader,
        StatePreCandidate,
        numStates
    }

    StateType state;

    // isLearner is true if the local raft node is a learner.
    boolean isLeader;

    // the leader id
    long leader;

    Map<Long, Boolean> votes;
    ArrayList<Raftpb.Message> msgs;
    long lead = None;

    // leadTransferee is id of the leader transfer target when its value is not zero.
    // Follow the procedure defined in raft thesis 3.10.
    long leadTransferee;

    // Only one conf change may be pending (in the log, but not yet
    // applied) at a time. This is enforced via pendingConfIndex, which
    // is set to a value >= the log index of the latest pending
    // configuration change (if any). Config changes are only allowed to
    // be proposed if the leader's applied index is greater than this
    // value.
    long pendingConfIndex;

    ReadOnly readOnly;

    // number of ticks since it reached last electionTimeout when it is leader
    // or candidate.
    // number of ticks since it reached last electionTimeout or received a
    // valid message from current leader when it is a follower.
    int electionElapsed;

    // number of ticks since it reached last heartbeatTimeout.
    // only leader keeps heartbeatElapsed.
    int heartbeatElapsed;

    boolean checkQuorum, preVote;

    int heartbeatTimeout, electionTimeout;

    // resetRandomizedElectionTimeout is a random number between
    // [electiontimeout, 2 * electiontimeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    int randomizedElectionTimeout;

    boolean disableProposalForwarding;

    interface Tick {
        void run() throws Exception;
    }

    Tick tick;

    public interface StepFunc {
        void apply(Raft raft, Raftpb.Message m) throws Exception;
    }

    StepFunc step;
    RaftLogger logger;


    final static int intn(int seed) {
        return ThreadLocalRandom.current().nextInt(seed);
    }

    /**
     * @param c
     * @return
     * @throws Exception
     * @finished
     */
    public static Raft newRaft(Config c) throws Exception {
        c.validate();
        RaftLog raftLog = RaftLog.newLogWithSize(c.getStorage(), c.getLogger(), c.getMaxSizePerMsg());
        Storage.State state = c.getStorage().initialSate();
        Raftpb.HardState hs = state.getHardState();
        Raftpb.ConfState cs = state.getConfState();
        List<Long> peers = c.getPeers();
        List<Long> leaders = c.getLeaders();
        if (Util.len(cs.getNodes()) > 0 || Util.len(cs.getLeaders()) > 0) {
            if (peers.size() > 0 || leaders.size() > 0) {
                // TODO(bdarnell): the peers argument is always nil except in
                // tests; the argument should be removed and these tests should be
                // updated to specify their nodes through a snapshot.
                Util.panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
            }
            peers = cs.getNodes();
            leaders = cs.getLeaders();
        }
        Raft r = Raft.builder()
                .id(c.getId())
                .lead(None)
                .isLeader(false)
                .raftLog(raftLog)
                .maxMsgSize(c.getMaxSizePerMsg())
                .maxInflight(c.getMaxInflightMsgs())
                .prs(new HashMap<>())
                .leaderPrs(new HashMap<>())
                .electionTimeout(c.getElectionTick())
                .heartbeatTimeout(c.getHeartbeatTick())
                .logger(c.getLogger())
                .checkQuorum(c.isCheckQuorum())
                .preVote(c.isPreVote())
                .readOnly(new ReadOnly(c.getReadOnlyOption()))
                .disableProposalForwarding(c.isDisableProposalForwarding())
                .msgs(new ArrayList<>())
                .build();
        for (long p : peers) {
            r.prs.put(p, Process.builder().state(Process.ProgressStateType.ProgressStateProbe).next(1).ins(Inflights.newInflights(r.getMaxInflight())).build());
        }
        for (long p : leaders) {
            Process process = r.prs.get(p);
            if (process != null) {
                Util.panic(String.format("node %x is in both learner and peer list", p));
            }
            r.leaderPrs.put(p, Process.builder().state(Process.ProgressStateType.ProgressStateProbe).next(1).ins(Inflights.newInflights(r.getMaxInflight())).isLeader(true).build());
            if (r.id == p) {
                r.setLeader(true);
            }
        }

        if (!Util.isHardStateEqual(hs, Node.emptyState)) {
            r.loadState(hs);
        }
        if (c.getApplied() > 0) {
            raftLog.appliesTo(c.getApplied());
        }
        r.becomeFollower(r.getTerm(), None);
        List<String> nodeStrs = new ArrayList<>();

        for (Long node : r.nodes()) {
            nodeStrs.add(String.format("%x", node));
        }
        r.logger.infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
                r.id, String.join(",", nodeStrs), r.getTerm(), r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm());


        return r;
    }

    /**
     * @return
     * @finished
     */
    public List<Long> nodes() {
        List<Long> nodes = new ArrayList<>(this.prs.size());
        nodes.addAll(this.prs.keySet());
        Collections.sort(nodes);
        return nodes;
    }

    /**
     * @return
     * @finished
     */
    public List<Long> leaderNodes() {
        List<Long> nodes = new ArrayList<>(this.prs.size());
        nodes.addAll(this.leaderPrs.keySet());
        Collections.sort(nodes);
        return nodes;
    }

    /**
     * @return
     * @finished
     */
    public boolean hasLeader() {
        return this.lead != None;
    }

    /**
     * @return
     * @finished
     */
    public Node.SoftState softState() {
        return new Node.SoftState(lead, raftState);
    }

    /**
     * @return
     * @finished
     */
    public Raftpb.HardState hardState() {
        return new Raftpb.HardState(term, vote, this.raftLog.committed);
    }

    /**
     * @finished
     * @param r
     * @param m
     * @throws Exception
     */
    private void stepLeader(Raft r, Raftpb.Message m) throws Exception {
        // These message types do not require any progress for m.From.
        switch (m.getType()) {
            case MsgBeat:
                this.bcastHeartBeat();
                return;
            case MsgCheckQuorum: {
                if (!this.checkQuorumActive()) {
                    r.logger.waringf("%x stepped down to follower since quorum is not active", r.id);
                    this.becomeFollower(this.term, None);
                }
                return;
            }
            case MsgProp: {
                if (Util.len(m.getEntries()) == 0) {
                    r.logger.panicf("%x stepped empty MsgProp", r.id);
                }
                if (!this.prs.containsKey(this.id)) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    throw ErrProposalDropped;
                }
                if (this.leadTransferee != None) {
                    r.logger.debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.term, r.leadTransferee);
                    throw ErrProposalDropped;
                }
                List<Raftpb.Entry> entries = m.getEntries();
                int size = entries.size();
                for (int i = 0; i < size; i++) {
                    Raftpb.Entry e = entries.get(i);
                    if (e.getType() == Raftpb.EntryType.ConfChange) {
                        if (r.pendingConfIndex > r.raftLog.applied) {
                            r.logger.infof("propose conf %s ignored since pending unapplied configuration [index %d, applied %d]",
                                    e.toString(), r.pendingConfIndex, r.raftLog.applied);
                            entries.set(i, Raftpb.Entry.builder().type(Raftpb.EntryType.Normal).build());
                        } else {
                            this.pendingConfIndex = this.raftLog.lastIndex() + i + 1;
                        }
                    }
                }
                this.appendEntry(m.getEntries());
                this.bcastAppend();
                return;
            }
            case MsgReadIndex: {
                if (this.quorum() > 1) {
                    long term = 0L;
                    Exception e = null;
                    try {
                        term = this.raftLog.term(this.raftLog.getCommitted());
                    } catch (Exception e1) {
                        e = e1;
                    }
                    if (this.raftLog.zeroTermOnErrCompacted(term, e) != this.term) {
                        // Reject read only request when this leader has not committed any log entry at its term.
                        return;
                    }

                    // thinking: use an interally defined context instead of the user given context.
                    // We can express this in terms of the term and index instead of a user-supplied value.
                    // This would allow multiple reads to piggyback on the same message.
                    switch (this.readOnly.option) {
                        case ReadOnlySafe:
                            this.readOnly.addRequest(this.raftLog.getCommitted(), m);
                            this.bcastHeartbeatWithCtx(m.getEntries().get(0).getData());
                            break;
                        case ReadOnlyLeaseBased:
                            long ri = this.raftLog.getCommitted();// from local member
                            if (m.getFrom() == None || m.getFrom() == this.id) {
                                this.readStates.add(ReadOnly.ReadState.builder()
                                        .index(this.raftLog.getCommitted())
                                        .requestCtx(m.getEntries().get(0).getData())
                                        .build());
                            } else {
                                this.send(Raftpb.Message.builder()
                                        .to(m.getFrom())
                                        .type(Raftpb.MessageType.MsgReadIndexResp)
                                        .index(ri)
                                        .entries(m.getEntries())
                                        .build());
                            }
                            break;
                    }

                } else {
                    this.readStates.add(ReadOnly.ReadState.builder()
                            .index(this.raftLog.getCommitted())
                            .requestCtx(m.getEntries().get(0).getData())
                            .build());
                }
                return;
            }
            default:
        }

        // All other message types require a progress for m.From (pr).
        Process pr = this.getProgress(m.getFrom());
        if (pr == null) {
            r.logger.debugf("%x no progress available for %x", r.id, m.getFrom());
            return;
        }
        switch (m.getType()) {
            case MsgAppResp: {
                pr.setRecentActive(true);
                if (m.isReject()) {
                    RaftLogger.raftLogger.debugf("%x received msgApp rejection(lastindex: %d) from %x for index %d",
                            r.id, m.getRejectHint(), m.from, m.index);
                    if (pr.maybeDecrTo(m.getIndex(), m.getRejectHint())) {
                        RaftLogger.raftLogger.debugf("%x decreased progress of %x to [%s]", r.id, m.from, pr);
                        if (pr.getState() == Process.ProgressStateType.ProgressStateReplicate) {
                            pr.becomeProbe();
                        }
                        this.sendAppend(m.getFrom());
                    }
                } else {
                    boolean oldPaused = pr.isPaused();
                    if (pr.maybeUpdate(m.getIndex())) {
                        switch (pr.getState()) {
                            case ProgressStateProbe:
                                pr.becomeReplicate();
                                break;
                            case ProgressStateReplicate:
                                pr.getIns().freeTo(m.getIndex());
                                break;
                            case ProgressStateSnapshot:
                                if (pr.needSnapshotAbort()) {
                                    RaftLogger.raftLogger.debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.from, pr);
                                    pr.becomeProbe();
                                }
                                break;
                        }
                        if (this.maybeCommit()) {
                            this.bcastAppend();
                        } else if (oldPaused) {
                            // If we were paused before, this node may be missing the
                            // latest commit index, so send it.
                            this.sendAppend(m.getFrom());
                        }
                        // We've updated flow control information above, which may
                        // allow us to send multiple (size-limited) in-flight messages
                        // at once (such as when transitioning from probe to
                        // replicate, or when freeTo() covers multiple messages). If
                        // we have more entries to send, send as many messages as we
                        // can (without sending empty messages for the commit index)
                        while (r.maybeSendAppend(m.getFrom(), false)) ;
                        // Transfer leadership is in progress.
                        if (m.getFrom() == r.getLeadTransferee() && pr.getMatch() == this.raftLog.lastIndex()) {
                            this.sendTimeoutNow(m.getFrom());
                        }
                    }
                }
            }
            break;
            case MsgHeartbeatResp: {
                pr.setRecentActive(true);
                pr.resume();

                // free one slot for the full inflights window to allow progress.
                if (pr.getState() == Process.ProgressStateType.ProgressStateReplicate && pr.getIns().full()) {
                    pr.getIns().freeFirstOne();
                }
                if (pr.getMatch() < this.raftLog.lastIndex()) {
                    this.sendAppend(m.getFrom());
                }
                if (this.readOnly.getOption() != ReadOnlyOption.ReadOnlySafe || m.getContext().length == 0) {
                    return;
                }
                int ackCount = this.readOnly.recvAck(m);
                if (ackCount < this.quorum()) {
                    return;
                }
                List<ReadOnly.ReadIndexStatus> rss = this.readOnly.advance(m);
                int size = rss.size();
                for (int i = 0; i < size; i++) {
                    ReadOnly.ReadIndexStatus rs = rss.get(i);
                    Raftpb.Message req = rs.getReq();
                    if (req.getFrom() == None || req.getFrom() == this.id) {
                        this.readStates.add(ReadOnly.ReadState.builder()
                                .index(rs.getIndex())
                                .requestCtx(req.getEntries().get(0).getData())
                                .build());
                    } else {
                        this.send(Raftpb.Message.builder()
                                .to(req.getFrom())
                                .type(Raftpb.MessageType.MsgReadIndexResp)
                                .index(rs.getIndex())
                                .entries(req.getEntries())
                                .build());
                    }
                }
            }
            break;
            case MsgSnapStatus: {
                if (pr.getState() != Process.ProgressStateType.ProgressStateSnapshot) {
                    return;
                }
                if (!m.isReject()) {
                    pr.becomeProbe();
                    RaftLogger.raftLogger.debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.getFrom(), pr);
                } else {
                    pr.snapshotFailure();
                    pr.becomeProbe();
                    r.logger.debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.getFrom(), pr);
                }
                // If snapshot finish, wait for the msgAppResp from the remote node before sending
                // out the next msgApp.
                // If snapshot failure, wait for a heartbeat interval before next try
                pr.pause();
            }
            break;
            case MsgUnreachable: {
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgApp is lost.
                if (pr.getState() == Process.ProgressStateType.ProgressStateReplicate) {
                    pr.becomeProbe();
                }
                r.logger.debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.getFrom(), pr);
            }
            break;
            case MsgTransferLeader: {
                if (pr.isLeader()) {
                    r.logger.debugf("%x is learner. Ignored transferring leadership", r.id);
                    return;
                }
                long leadTransFeree = m.getFrom();
                long lastLeadTransferee = this.getLeadTransferee();
                if (lastLeadTransferee != None) {
                    if (lastLeadTransferee == leadTransFeree) {
                        r.logger.infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
                                r.id, r.getTerm(), leadTransferee, leadTransferee);
                        return;
                    }
                    this.abortLeaderTransfer();
                    r.logger.infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.term, lastLeadTransferee);
                }
                if (leadTransFeree == this.id) {
                    r.logger.debugf("%x is already leader. Ignored transferring leadership to self", r.id);
                    return;
                }
                // Transfer leadership to third party.
                r.logger.infof("%x [term %d] starts to transfer leadership to %x", r.id, r.term, leadTransferee);
                // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
                this.electionElapsed = 0;
                this.leadTransferee = leadTransFeree;
                if (pr.getMatch() == this.getRaftLog().lastIndex()) {
                    this.sendTimeoutNow(leadTransFeree);
                    r.logger.infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee);
                } else {
                    this.sendAppend(leadTransFeree);
                }
            }
            break;
        }
        return;
    }

    /**
     *stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
     *whether they respond to MsgVoteResp or MsgPreVoteResp.
     *
     * @finished
     * @param r
     * @param m
     * @throws Exception
     */
    private void stepCandidate(Raft r, Raftpb.Message m) throws Exception {
        // Only handle vote responses corresponding to our candidacy (while in
        // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
        // our pre-candidate state).
        Raftpb.MessageType myVoteRespType;
        if (this.getState() == StateType.StatePreCandidate) {
            myVoteRespType = Raftpb.MessageType.MsgPreVoteResp;
        } else {
            myVoteRespType = Raftpb.MessageType.MsgVoteResp;
        }
        switch (m.getType()) {
            case MsgProp:
                r.logger.infof("%x no leader at term %d; dropping proposal", r.id, r.term);
                throw ErrProposalDropped;
            case MsgApp: {
                this.becomeFollower(m.getTerm(), m.getFrom());// always m.Term == r.Term
                this.handleAppendEntries(m);
                break;
            }
            case MsgHeartbeat: {
                this.becomeFollower(m.getTerm(), m.getFrom()); // always m.Term == r.Term
                this.handleHeartbeat(m);
                break;
            }
            case MsgSnap: {
                this.becomeFollower(m.getTerm(), m.getFrom());
                this.handleSnapshot(m);
                break;
            }
            case MsgTimeoutNow: {
                r.logger.debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.term, r.state, m.from);
                break;
            }
            default:
                if (myVoteRespType == m.getType()){
                    {
                        int gr = this.poll(m.getFrom(), m.getType(), !m.isReject());
                        r.logger.infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.type, r.votes.size()-gr);
                        int q = this.quorum();
                        if (q == gr) {
                            if (this.state == StateType.StatePreCandidate){
                                this.campaign(CampaignType.campaignElection);
                            }else{
                                this.becomeLeader();
                                this.bcastAppend();
                            }
                        } else if (q == (this.votes.size() - gr)) {
                            // pb.MsgPreVoteResp contains future term of pre-candidate
                            // m.Term > r.Term; reuse r.Term
                            this.becomeFollower(this.getTerm(), None);
                        }
                        break;
                    }
                }
        }
        return;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    /**
     * @finished
     * @param r
     * @param m
     * @throws Exception
     */
    private void stepFollower(Raft r, Raftpb.Message m) throws Exception {
        switch (m.getType()) {
            case MsgProp: {
                if (this.lead == None) {
                    r.logger.infof("%x no leader at term %d; dropping proposal", r.id, r.term);
                    throw ErrProposalDropped;
                } else if (this.isDisableProposalForwarding()) {
                    r.logger.infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.term);
                    throw ErrProposalDropped;
                }
                m.setTo(this.lead);
                this.send(m);
                break;
            }
            case MsgApp: {
                this.electionElapsed = 0;
                this.lead = m.getFrom();
                this.handleAppendEntries(m);
                break;
            }
            case MsgHeartbeat: {
                this.electionElapsed = 0;
                this.lead = m.getFrom();
                this.handleHeartbeat(m);
                break;
            }
            case MsgSnap: {
                this.electionElapsed = 0;
                this.lead = m.getFrom();
                this.handleSnapshot(m);
                break;
            }
            case MsgTransferLeader: {
                if (this.lead == None) {
                    r.logger.infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.term);
                    return;
                }
                m.setTo(this.lead);
                this.send(m);
                break;
            }
            case MsgTimeoutNow: {
                if (this.promotable()) {
                    r.logger.infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.",
                            r.id, r.term, m.from);
                    // Leadership transfers never use pre-vote even if r.preVote is true; we
                    // know we are not recovering from a partition so there is no need for the
                    // extra round trip.
                    this.campaign(CampaignType.campaignTransfer);
                } else {
                    r.logger.infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.from);
                }
                break;
            }
            case MsgReadIndex: {
                if (this.lead == None) {
                    r.logger.infof("%x no leader at term %d; dropping index reading msg", r.id, r.term);
                    return;
                }
                m.setTo(this.lead);
                this.send(m);
                break;
            }
            case MsgReadIndexResp: {
                if (m.getEntries().size() != 1) {
                    r.logger.errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.from, Util.len(m.entries));
                    return;
                }
                this.readStates.add(ReadOnly.ReadState.builder()
                        .index(m.getIndex())
                        .requestCtx(m.getEntries().get(0).getData())
                        .build());
                break;
            }
        }
        return;
    }

    /**
     * @param term
     * @param lead
     * @finished
     */
    void becomeFollower(long term, long lead) {
        Raft r = this;
        r.step = this::stepFollower;
        r.reset(term);
        r.tick = this::tickElection;
        r.lead = lead;
        r.state = StateType.StateFollower;
        r.logger.infof("%x became follower at term %d", r.id, r.getTerm());
    }

    /**
     * @finished
     */
    void becomeCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (this.state == StateType.StateLeader) {
            Util.panic("invalid transition [leader -> candidate]");
        }
        this.step = this::stepCandidate;
        this.reset(this.term + 1);
        this.tick = this::tickElection;
        this.vote = this.id;
        this.state = StateType.StateCandidate;
        this.logger.infof("%x became candidate at term %d", this.id, this.getTerm());
    }

    /**
     * @finished
     */
    void becomePreCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (this.state == StateType.StateLeader) {
            Util.panic("invalid transition [leader -> pre-candidate]");
        }
        // Becoming a pre-candidate changes our step functions and state,
        // but doesn't change anything else. In particular it does not increase
        // r.Term or change r.Vote.
        this.step = this::stepCandidate;
        this.votes = new HashMap<>();
        this.tick = this::tickElection;
        this.state = StateType.StatePreCandidate;
        this.logger.infof("%x became pre-candidate at term %d", this.id, this.getTerm());
    }

    /**
     * @throws Exception
     * @finished
     */
    void becomeLeader() throws Exception {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (this.state == StateType.StateFollower) {
            Util.panic("invalid transition [follower -> leader]");
        }
        this.step = this::stepLeader;
        this.reset(this.term);
        this.tick = this::tickHeartbeat;
        this.lead = this.id;
        this.state = StateType.StateLeader;

        this.pendingConfIndex = this.raftLog.lastIndex();
        this.appendEntry(Arrays.asList(Raftpb.Entry.builder().data(null).build()));
        this.logger.infof("%x became leader at term %d", this.id, this.getTerm());
    }

    /**
     * @param es
     * @throws Exception
     * @finished
     */
    public void appendEntry(List<Raftpb.Entry> es) throws Exception {
        long li = this.raftLog.lastIndex();
        int size = es.size();
        for (int i = 0; i < size; i++) {
            Raftpb.Entry entry = es.get(i);
            entry.setTerm(this.term);
            entry.setIndex(li + 1 + i);
        }
        // use latest "last" index after truncate/append
        li = this.raftLog.append(es);
        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        this.getProgress(this.id).maybeUpdate(li);
        this.maybeCommit();
    }

    /**
     * @param t
     * @throws Exception
     * @finished
     */
    void campaign(CampaignType t) throws Exception {
        long term = 0L;
        Raftpb.MessageType voteMsg;
        if (t == CampaignType.campaignPreElection) {
            this.becomePreCandidate();
            voteMsg = Raftpb.MessageType.MsgPreVote;
            term = this.term + 1;
        } else {
            this.becomeCandidate();
            voteMsg = Raftpb.MessageType.MsgVote;
            term = this.term;
        }
        if (this.quorum() == this.poll(this.id, Util.voteRespMsgType(voteMsg), true)) {
            if (t == CampaignType.campaignPreElection) {
                this.campaign(CampaignType.campaignElection);
            } else {
                this.becomeLeader();
            }
            return;
        }
        for (Long id : this.prs.keySet()) {
            if (id == this.id) {
                continue;
            }
            Raft r = this;
            this.logger.infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
                    r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.getTerm());
            byte[] ctx = null;
            if (t == CampaignType.campaignTransfer) {
                ctx = campaignTransfer;
            }
            this.send(Raftpb.Message.builder()
                    .term(term)
                    .to(id)
                    .type(voteMsg)
                    .index(this.raftLog.lastIndex())
                    .logTerm(this.raftLog.lastTerm())
                    .context(ctx)
                    .build());
        }
    }

    /**
     * @param id
     * @param t
     * @param v
     * @return
     * @finished
     */
    int poll(long id, Raftpb.MessageType t, boolean v) {
        Raft r = this;
        if (v) {
            r.logger.infof("%x received %s from %x at term %d", r.id, t, id, r.getTerm());
        } else {
            r.logger.infof("%x received %s rejection from %x at term %d", r.id, t, id, r.getTerm());
        }
        if (!this.votes.containsKey(id)) {
            this.votes.put(id, v);
        }
        int granted = 0;
        for (Boolean value : this.votes.values()) {
            if (value) {
                ++granted;
            }
        }
        return granted;
    }

    /**
     * @param m
     * @fininshed
     */
    private void send(Raftpb.Message m) {
        m.setFrom(this.id);
        switch (m.getType()) {
            case MsgVote:
            case MsgVoteResp:
            case MsgPreVote:
            case MsgPreVoteResp: {
                if (m.getTerm() == 0L) {
                    // All {pre-,}campaign messages need to have the term set when
                    // sending.
                    // - MsgVote: m.Term is the term the node is campaigning for,
                    //   non-zero as we increment the term when campaigning.
                    // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                    //   granted, non-zero for the same reason MsgVote is
                    // - MsgPreVote: m.Term is the term the node will campaign,
                    //   non-zero as we use m.Term to indicate the next term we'll be
                    //   campaigning for
                    // - MsgPreVoteResp: m.Term is the term received in the original
                    //   MsgPreVote if the pre-vote was granted, non-zero for the
                    //   same reasons MsgPreVote is
                    Util.panic("term should be set when sending %s", m.getType());
                }
            }
            break;
            default: {
                if (m.getTerm() != 0L) {
                    Util.panic("term should not be set when sending %s (was %d)", m.getType(), m.getTerm());
                }
                // do not attach term to MsgProp, MsgReadIndex
                // proposals are a way to forward to the leader and
                // should be treated as local message.
                // MsgReadIndex is also forwarded to leader.
                if (m.getType() != Raftpb.MessageType.MsgProp && m.getType() != Raftpb.MessageType.MsgReadIndex) {
                    m.setTerm(this.term);
                }
            }
            break;
        }
        this.msgs.add(m);
    }

    /**
     * @param m
     * @throws Exception
     * @finished
     */
    public void step(Raftpb.Message m) throws Exception {
        Raft r = this;
        // Handle the message term, which may result in our stepping down to a follower.
        long term = m.getTerm();
        Raftpb.MessageType type = m.getType();
        if (term == 0L) {

        } else if (term > this.term) {
            if (type == Raftpb.MessageType.MsgVote || type == Raftpb.MessageType.MsgPreVote) {
                boolean force = Arrays.equals(m.getContext(), campaignTransfer);
                boolean inLease = this.checkQuorum && this.lead != None && this.electionElapsed < this.electionTimeout;
                if (!force && inLease) {
                    // If a server receives a RequestVote request within the minimum election timeout
                    // of hearing from a current leader, it does not update its term or grant its vote
                    RaftLogger.raftLogger.infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
                            this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, m.type, m.from, m.logTerm, m.index, this.term, this.electionTimeout - this.electionElapsed);
                    return;
                }
            }
            if (type == Raftpb.MessageType.MsgPreVote) {
                // Never change our term in response to a PreVote
            } else if (type == Raftpb.MessageType.MsgPreVoteResp && !m.reject) {
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                RaftLogger.raftLogger.infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
                        this.id, this.term, m.type, m.from, m.term);
                if (type == Raftpb.MessageType.MsgApp || type == Raftpb.MessageType.MsgHeartbeat || type == Raftpb.MessageType.MsgSnap) {
                    this.becomeFollower(m.getTerm(), m.getFrom());
                } else {
                    this.becomeFollower(m.getTerm(), None);
                }
            }
        } else if (m.getTerm() < this.term) {
            if ((this.checkQuorum || this.preVote) && (m.getType() == Raftpb.MessageType.MsgHeartbeat || m.getType() == Raftpb.MessageType.MsgApp)) {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a
                // higher term, but if checkQuorum is true we may not advance the term on
                // MsgVote and must generate other messages to advance the term. The net
                // result of these two features is to minimize the disruption caused by
                // nodes that have been removed from the cluster's configuration: a
                // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                // but it will not receive MsgApp or MsgHeartbeat, so it will not create
                // disruptive term increases, by notifying leader of this node's activeness.
                // The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election ending
                // up with a higher term than leader, although it won't receive enough
                // votes to win the election. When it regains connectivity, this response
                // with "pb.MsgAppResp" of higher term would force leader to step down.
                // However, this disruption is inevitable to free this stuck node with
                // fresh election. This can be prevented with Pre-Vote phase.
                this.send(Raftpb.Message.builder().to(m.getFrom()).type(Raftpb.MessageType.MsgAppResp).build());
            } else if (type == Raftpb.MessageType.MsgPreVote) {
                // Before Pre-Vote enable, there may have candidate with higher term,
                // but less log. After update to Pre-Vote, the cluster may deadlock if
                // we drop messages with a lower term.
                RaftLogger.raftLogger.infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
                        this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, m.type, m.from, m.logTerm, m.index, m.term);
                this.send(Raftpb.Message.builder()
                        .to(m.getFrom())
                        .term(this.term)
                        .type(Raftpb.MessageType.MsgPreVoteResp)
                        .reject(true)
                        .build());
            } else {
                // ignore other cases
                RaftLogger.raftLogger.infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
                        this.id, this.term, m.type, m.from, m.term);
            }
            return;
        }
        switch (type) {
            case MsgHup:
                if (this.state != StateType.StateLeader) {
                    List<Raftpb.Entry> ents = Collections.EMPTY_LIST;
                    try {
                        ents = this.raftLog.slice(this.raftLog.applied + 1, this.raftLog.committed + 1, noLimit);
                    } catch (Exception err) {
                        this.logger.panicf("unexpected error getting unapplied entries (%v)", err);
                    }
                    int n = numOfPendingConf(ents);
                    if (n != 0 && this.raftLog.committed > this.raftLog.applied) {
                        this.logger.waringf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply",
                                this.id, this.term, n);
                        return;
                    }
                    this.logger.infof("%x is starting a new election at term %d", this.id, this.term);
                    if (this.preVote) {
                        this.campaign(CampaignType.campaignPreElection);
                    } else {
                        this.campaign(CampaignType.campaignElection);
                    }
                } else {
                    this.logger.debugf("%x ignoring MsgHup because already leader", this.id);
                }
                break;
            case MsgVote:
            case MsgPreVote:
                if (this.isLeader) {
                    // TODO: learner may need to vote, in case of node down when confchange.
                    r.logger.infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: learner can not vote",
                            r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.type, m.from, m.logTerm, m.index, r.term);
                    return;
                }
                // We can vote if this is a repeat of a vote we've already cast...
                boolean canVote =
                        (this.vote == m.getFrom()) ||
                                // ...we haven't voted and we don't think there's a leader yet in this term...
                                (this.vote == None && this.lead == None) ||
                                // ...or this is a PreVote for a future term...
                                (m.getType() == Raftpb.MessageType.MsgPreVote && m.getTerm() > this.term);
                // ...and we believe the candidate is up to date.
                if (canVote && this.raftLog.isUpToDate(m.getIndex(), m.getLogTerm())) {
                    r.logger.infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
                            r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.type, m.from, m.logTerm, m.index, r.term);
                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    this.send(Raftpb.Message.builder().to(m.getFrom()).term(m.getTerm()).type(Util.voteRespMsgType(m.getType())).build());
                    if (m.getType() == Raftpb.MessageType.MsgVote) {
                        this.electionElapsed = 0;
                        this.vote = m.getFrom();
                    }
                } else {
                    r.logger.infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
                            r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.type, m.from, m.logTerm, m.index, r.term);
                    this.send(Raftpb.Message.builder().to(m.getFrom()).term(this.term).type(m.type).reject(true).build());
                }
                break;
            default:
                this.step.apply(this, m);
                return;
        }
    }

    /**
     * @param id
     * @return
     * @finished
     */
    private Process getProgress(long id) {
        Process process = this.prs.get(id);
        if (process != null) return process;
        return this.leaderPrs.get(id);
    }

    /**
     * @param f
     * @finished
     */
    public void forEachProgress(BiConsumer<Long, Process> f) {
        for (Map.Entry<Long, Process> entry : this.prs.entrySet()) {
            f.accept(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Long, Process> entry : this.leaderPrs.entrySet()) {
            f.accept(entry.getKey(), entry.getValue());
        }
    }

    /**
     * @param f
     * @finished
     */
    public void forEachUpdateProgress(BiFunction<Long, Process, Process> f) {
        for (Map.Entry<Long, Process> entry : this.prs.entrySet()) {
            entry.setValue(f.apply(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<Long, Process> entry : this.leaderPrs.entrySet()) {
            entry.setValue(f.apply(entry.getKey(), entry.getValue()));
        }
    }

    /**
     * bcastAppend sends RPC, with entries to all peers that are not up-to-date
     * according to the progress recorded in r.prs.
     */
    public void bcastAppend() {
        this.forEachProgress((id, p) -> {
            if (id == this.getId()) {
                return;
            }
            this.sendAppend(id);
        });
    }

    /**
     * sendHeartbeat sends an empty MsgApp
     *
     * @param to
     * @param ctx
     * @finished
     */
    public void sendHeartbeat(long to, byte[] ctx) {
        // Attach the commit as min(to.matched, r.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        long commit = Math.min(this.getProgress(to).getMatch(), this.raftLog.getCommitted());
        this.send(Raftpb.Message.builder().to(to).type(Raftpb.MessageType.MsgHeartbeat).commit(commit).context(ctx).build());
    }

    /**
     * @param ctx
     * @finished
     */
    public void bcastHeartbeatWithCtx(byte[] ctx) {
        this.forEachProgress((id, p) -> {
            if (id == this.id) {
                return;
            } else {
                this.sendHeartbeat(id, ctx);
            }
        });
    }

    /**
     * bcastHeartbeat sends RPC, without entries to all the peers.
     *
     * @finished
     */
    public void bcastHeartBeat() {
        String lastCtx = this.getReadOnly().lastPendingRequestCtx();
        if (lastCtx == null || lastCtx.isEmpty()) {
            this.bcastHeartbeatWithCtx(null);
        } else {
            this.bcastHeartbeatWithCtx(lastCtx.getBytes());
        }
    }

    /**
     * sendAppend sends an append RPC with new entries (if any) and the
     * current commit index to the given peer.
     *
     * @param to
     * @return
     */
    public boolean sendAppend(long to) {
        return this.maybeSendAppend(to, true);
    }

    /**
     * maybeSendAppend sends an append RPC with new entries to the given peer,
     * if necessary. Returns true if a message was sent. The sendIfEmpty
     * argument controls whether messages with no entries will be sent
     * ("empty" messages are useful to convey updated Commit indexes, but
     * are undesirable when we're sending multiple messages in a batch).
     *
     * @param to
     * @param sendIfEmpty
     * @return
     * @finished
     */
    public boolean maybeSendAppend(long to, boolean sendIfEmpty) {
        Process pr = this.getProgress(to);
        if (pr.isPaused()) {
            return false;
        }
        Raftpb.Message.MessageBuilder mBuilder = Raftpb.Message.builder().to(to);
        Exception errt = null, erre = null;
        long term = 0;
        List<Raftpb.Entry> ents = null;
        Raftpb.Message m;
        try {
            term = this.raftLog.term(pr.getNext() - 1);
        } catch (Exception e) {
            errt = e;
        }
        try {
            ents = this.raftLog.entries(pr.getNext(), this.getMaxMsgSize());
        } catch (Exception e) {
            erre = e;
        }
        if (Util.len(ents) == 0 && !sendIfEmpty) {
            return false;
        }
        if (errt != null || erre != null) {
            if (!pr.isRecentActive()) {
                this.logger.debugf("ignore sending snapshot to %x since it is not recently active", to);
                return false;
            }
            mBuilder.type(Raftpb.MessageType.MsgSnap);
            Exception err = null;
            Raftpb.Snapshot snapshot = null;
            try {
                snapshot = this.raftLog.snapshot();
            } catch (Exception e) {
                err = e;
            }
            if (err != null) {
                if (err == Storage.ErrSnapshotTemporarilyUnavailable) {
                    this.logger.debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", this.id, to);
                    return false;
                }
                Util.panic(err);// TODO(bdarnell)
            }
            if (Util.isEmptySnap(snapshot)) {
                Util.panic("need non-empty snapshot");
            }
            mBuilder.snapshot(snapshot);
            long sindex = snapshot.getMetadata().getIndex();
            long sterm = snapshot.getMetadata().getTerm();
            Raft r = this;
            try {
                this.logger.debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
                        r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr);
            } catch (Exception e) {
                e.printStackTrace();
            }
            pr.becomeSnapshot(sindex);
            this.logger.debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr);
        } else {
            m = mBuilder.type(Raftpb.MessageType.MsgApp)
                    .index(pr.getNext() - 1)
                    .logTerm(term)
                    .entries(ents)
                    .commit(this.raftLog.getCommitted()).build();
            if (Util.len(m.getEntries()) != 0) {
                switch (pr.getState()) {
                    case ProgressStateProbe:
                        pr.pause();
                        break;
                    case ProgressStateReplicate:
                        List<Raftpb.Entry> entries = m.getEntries();
                        long last = entries.get(entries.size() - 1).getIndex();
                        pr.optimisticUpdate(last);
                        pr.getIns().add(last);
                        break;
                    case ProgressStateSnapshot:
                        break;
                    default:
                        this.logger.panicf("%x is sending append in unhandled state %s", this.id, pr.getState());
                }
            }
        }
        m = mBuilder.build();
        this.send(m);
        return true;
    }

    /**
     * @finished
     * @param id
     * @param match
     * @param next
     * @param isLeader
     */
    public void setProgress(long id, long match, long next, boolean isLeader) {
        if (!isLeader) {
            this.leaderPrs.remove(id);
            this.prs.put(id, Process.builder().state(Process.ProgressStateType.ProgressStateProbe).next(next).match(match).ins(Inflights.newInflights(this.maxInflight)).build());
            return;
        }
        if (this.prs.containsKey(id)) {
            Util.panic("%x unexpected changing from voter to learner for %x", this.id, id);
        }
        this.prs.put(id, Process.builder().state(Process.ProgressStateType.ProgressStateProbe).next(next).match(match).ins(Inflights.newInflights(this.maxInflight)).isLeader(true).build());
    }

    /**
     * @finished
     * @param id
     */
    public void delProgress(long id) {
        this.prs.remove(this.prs, id);
        this.leaderPrs.remove(this.prs, id);
    }

    /**
     * @finished
     * @param id
     * @param isLeader
     */
    public void addNodeOrLearnerNode(long id, boolean isLeader) {
        Process pr = this.getProgress(id);
        if (pr == null) {
            this.setProgress(id, 0, this.raftLog.lastIndex() + 1, isLeader);
        } else {
            if (isLeader && !pr.isLeader()) {
                // can only change Learner to Voter
                this.logger.infof("%x ignored addLearner: do not support changing %x from raft peer to learner.", this.id, id);
                return;
            }
            if (isLeader == pr.isLeader()) {
                // Ignore any redundant addNode calls (which can happen because the
                // initial bootstrapping entries are applied twice).
                return;
            }
            // change Learner to Voter, use origin Learner progress
            this.leaderPrs.remove(id);
            pr.setLeader(false);
            this.prs.put(id, pr);
        }
        if (this.id == id) {
            this.isLeader = isLeader;
        }
        // When a node is first added, we should mark it as recently active.
        // Otherwise, CheckQuorum may cause us to step down if it is invoked
        // before the added node has a chance to communicate with us.
        pr = this.getProgress(id);
        pr.setRecentActive(true);
    }

    /**
     * promotable indicates whether state machine can be promoted to leader,
     * which is true when its own id is in progress list.
     * @return
     */
    public boolean promotable() {
        return this.prs.containsKey(this.id);
    }

    /**
     * @finished
     * @param id
     */
    public void addNode(long id) {
        this.addNodeOrLearnerNode(id, false);
    }

    /**
     * @finished
     * @param id
     */
    public void addLearner(long id) {
        this.addNodeOrLearnerNode(id, true);
    }

    /**
     * @return
     * @finished
     */
    public int quorum() {
        return this.prs.size() / 2 + 1;
    }

    /**
     * tickElection is run by followers and candidates after r.electionTimeout.
     *
     * @throws Exception
     * @finished
     */
    public void tickElection() throws Exception {
        this.electionElapsed++;
        if (this.promotable() && this.pastElectionTimeout()) {
            this.electionElapsed = 0;
            this.step(Raftpb.Message.builder().from(this.id).type(Raftpb.MessageType.MsgHup).build());
        }
    }

    /**
     * tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
     *
     * @throws Exception
     * @finished
     */
    public void tickHeartbeat() throws Exception {
        this.heartbeatElapsed++;
        this.electionElapsed++;
        if (this.electionElapsed >= this.electionTimeout) {
            this.electionElapsed = 0;
            if (this.checkQuorum) {
                this.send(Raftpb.Message.builder().from(this.id).type(Raftpb.MessageType.MsgCheckQuorum).build());
            }
            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (this.state == StateType.StateLeader && this.leadTransferee != None) {
                this.abortLeaderTransfer();
            }
        }
        if (this.state != StateType.StateLeader) {
            return;
        }
        if (this.heartbeatElapsed >= this.heartbeatTimeout) {
            this.heartbeatElapsed = 0;
            this.step(Raftpb.Message.builder().from(this.id).type(Raftpb.MessageType.MsgBeat).build());
        }
    }


    /**
     * maybeCommit attempts to advance the commit index. Returns true if
     * the commit index changed (in which case the caller should call
     * r.bcastAppend).
     *
     * @return
     * @throws Exception
     * @finished
     */
    public boolean maybeCommit() throws Exception {
        // Preserving matchBuf across calls is an optimization
        // used to avoid allocating a new slice on each call.
        int size = this.prs.size();
        if (this.matchBuf == null || this.matchBuf.length < size) {
            this.matchBuf = new long[size];
        }
        int misLength = size;
        long[] mis =  this.matchBuf;
        int idx = 0;
        for (Process p : this.prs.values()) {
            mis[idx] = p.getMatch();
            idx++;
        }
        Arrays.sort(mis,0,misLength);
        long mci = mis[misLength - this.quorum()];
        return this.raftLog.matchCommit(mci, this.getTerm());
    }

    /**
     * @finished
     * @param nodes
     * @param isLeader
     */
    public void restoreNode(List<Long> nodes, boolean isLeader) {
        for (long n : nodes) {
            long match = 0;
            long next = this.raftLog.lastIndex() + 1;
            if (n == this.id) {
                match = next - 1;
                this.isLeader = isLeader;
            }
            this.setProgress(n, match, next, isLeader);
            this.logger.infof("%x restored progress of %x [%s]", this.id, n, this.getProgress(n));
        }

    }

    /**
     * @param term
     * @finished
     */
    @SneakyThrows
    public void reset(long term) {
        if (this.term != term) {
            this.term = term;
            this.vote = None;
        }
        this.lead = None;

        this.electionElapsed = 0;
        this.heartbeatElapsed = 0;

        this.resetRandomizedElectionTimeout();

        this.abortLeaderTransfer();

        this.votes = new HashMap<>();
        this.forEachUpdateProgress((id, process) -> {
            try {
                Process pr = Process.builder()
                        .next(this.raftLog.lastIndex() + 1)
                        .state(process.getState())
                        .ins(Inflights.newInflights(this.maxInflight))
                        .isLeader(process.isLeader())
                        .build();
                if (id == this.id) {
                    process.match = this.raftLog.lastIndex();
                }
                return pr;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        this.pendingConfIndex = 0L;
        this.readOnly = ReadOnly.newReadOnly(this.readOnly.option);
    }

    /**
        checkQuorumActive returns true if the quorum is active from
        the view of the local raft state machine. Otherwise, it returns
     ```false.
     ```checkQuorumActive also resets all RecentActive to false.
     *@finished
     * @return
     */
    public boolean checkQuorumActive() {
        AtomicInteger act = new AtomicInteger(0);
        this.forEachProgress((id, pr) -> {
            if (id == this.id) {
                act.incrementAndGet();
                return;
            }
            if (pr.isRecentActive() && !pr.isLeader()) {
                act.incrementAndGet();
            }
            pr.setRecentActive(false);
        });
        return act.get() >= this.quorum();
    }

    /**
     * @finished
     * @param state
     */
    public void loadState(Raftpb.HardState state) {
        if (state.getCommit() < this.raftLog.getCommitted() ||
                state.getCommit() > this.raftLog.lastIndex()) {
            this.logger.panicf("%x state.commit %d is out of range [%d, %d]", this.id, state.commit, this.raftLog.committed, this.raftLog.lastIndex());
        }
        this.raftLog.setCommitted(state.getCommit());
        this.term = state.getTerm();
        this.vote = state.getVote();
    }

    private void setTimeoutNow(long to) {
        this.send(Raftpb.Message.builder().to(to).type(Raftpb.MessageType.MsgTimeoutNow).build());
    }

    /**
     pastElectionTimeout returns true iff r.electionElapsed is greater
     than or equal to the randomized election timeout in
     [electiontimeout, 2 * electiontimeout - 1].
     * @finished
     * @return
     */
    private boolean pastElectionTimeout() {
        return this.electionElapsed >= this.randomizedElectionTimeout;
    }

    /**
     * @finished
     */
    private void resetRandomizedElectionTimeout() {
        this.randomizedElectionTimeout =
                this.electionTimeout +
                        ThreadLocalRandom.current().nextInt(this.electionTimeout);

    }

    /**
     * @finished
     * @param id
     * @throws Exception
     */
    public void removeNode(long id) throws Exception {
        this.delProgress(id);

        // do not try to commit or abort transferring if there is no nodes in the cluster.
        if (this.prs.isEmpty() && this.leaderPrs.isEmpty()) {
            return;
        }
        // The quorum size is now smaller, so see if any pending entries can
        // be committed.
        if (this.maybeCommit()) {
            this.bcastAppend();
        }
        // If the removed node is the leadTransferee, then abort the leadership transferring.
        if (this.state == StateType.StateLeader && this.leadTransferee == id) {
            this.abortLeaderTransfer();
        }
    }

    /**
     * @finished
     * @param m
     * @throws Exception
     */
    public void handleAppendEntries(Raftpb.Message m) throws Exception {
        if (m.getIndex() < this.raftLog.committed) {
            this.send(Raftpb.Message.builder().to(m.getFrom()).type(Raftpb.MessageType.MsgAppResp).index(this.raftLog.committed).build());
            return;
        }
        RaftLog.MaybeAppendResult mlastIndex = this.raftLog.maybeAppend(m.getIndex(), m.getLogTerm(), m.getCommit(), m.getEntries());
        if (mlastIndex.ok) {
            this.send(Raftpb.Message.builder().to(m.getFrom()).type(Raftpb.MessageType.MsgAppResp).index(mlastIndex.lastnewi).build());
        } else {
            this.send(Raftpb.Message.builder()
                    .to(m.getFrom())
                    .type(Raftpb.MessageType.MsgAppResp)
                    .index(m.getIndex())
                    .reject(true)
                    .rejectHint(this.raftLog.lastIndex()).build());
        }
    }

    /**
     * @finished
     * @param m
     */
    public void handleHeartbeat(Raftpb.Message m) {
        this.raftLog.commitTo(m.getCommit());
        this.send(Raftpb.Message.builder()
                .to(m.getFrom())
                .type(Raftpb.MessageType.MsgHeartbeatResp)
                .context(m.getContext())
                .build());
    }

    /**
     * restore recovers the state machine from a snapshot. It restores the log and the
     * configuration of state machine.
     *
     * @finished
     * @param s
     * @return
     */
    public boolean restore(Raftpb.Snapshot s) {
        Raft r = this;
        if (s.getMetadata().getIndex() <= this.raftLog.getCommitted()) {
            return false;
        }
        if (this.raftLog.matchTerm(s.getMetadata().getIndex(), s.getMetadata().getTerm())) {
            RaftLogger.raftLogger.infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
                    this.id, this.raftLog.committed, this.raftLog.lastIndex(), this.raftLog.lastTerm(), s.getMetadata().getIndex(), s.getMetadata().getTerm());
            this.raftLog.commitTo(s.getMetadata().getIndex());
            return false;
        }
        // The normal peer can't become learner.
        if (!this.isLeader()) {
            List<Long> leaders = s.getMetadata().getConfState().getLeaders();
            for (long id : leaders) {
                if (id == this.id) {
                    RaftLogger.raftLogger.errorf("%x can't become learner when restores snapshot [index: %d, term: %d]", this.id, s.getMetadata().getIndex(), s.getMetadata().getTerm());
                    return false;
                }
            }
        }

        r.logger.infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
                r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.metadata.index, s.metadata.term);


        this.raftLog.restore(s);
        this.prs = new HashMap<>();
        this.leaderPrs = new HashMap<>();
        this.restoreNode(s.getMetadata().getConfState().getNodes(), false);
        this.restoreNode(s.getMetadata().getConfState().getLeaders(), true);
        return true;
    }

    public void restore(long[] nodes, boolean isLeader) {
        int size = nodes.length;
        for (int i = 0; i < size; i++) {
            long n = nodes[i];
            long match = 0;
            long next = this.raftLog.lastIndex();

            if (n == this.id) {
                match = next - 1;
                this.isLeader = isLeader;
            }
            this.setProgress(n, match, next, isLeader);
        }
    }

    /**
     *
     * @param m
     * @throws Exception
     */
    public void handleSnapshot(Raftpb.Message m) throws Exception {
        Raft r = this;
        long sindex = m.getSnapshot().getMetadata().getIndex();
        long sterm = m.getSnapshot().getMetadata().getTerm();
        if (this.restore(m.getSnapshot())) {
            r.logger.infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
                    r.id, r.raftLog.committed, sindex, sterm);
            this.send(Raftpb.Message.builder()
                    .to(m.getFrom())
                    .type(Raftpb.MessageType.MsgAppResp)
                    .index(this.raftLog.lastIndex())
                    .build());
        } else {
            r.logger.infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
                    r.id, r.raftLog.committed, sindex, sterm);
            this.send(Raftpb.Message.builder()
                    .to(m.getFrom())
                    .type(Raftpb.MessageType.MsgAppResp)
                    .index(this.raftLog.getCommitted())
                    .build());
        }
    }

    /**
     * @fininsed
     * @param to
     */
    public void sendTimeoutNow(long to) {
        this.send(Raftpb.Message.builder().to(to).type(Raftpb.MessageType.MsgTimeoutNow).build());
    }

    /**
     * @finished
     */
    public void abortLeaderTransfer() {
        this.leadTransferee = None;
    }

    /**
     * @finished
     * @param ents
     * @return
     */
    public int numOfPendingConf(List<Raftpb.Entry> ents) {
        if (ents == null) {
            ents = Collections.EMPTY_LIST;
        }
        int n = 0;
        for (Raftpb.Entry ent : ents) {
            if (ent.getType() == Raftpb.EntryType.ConfChange) {
                ++n;
            }
        }
        return n;
    }

    // Possible values for CampaignType
    // campaignPreElection represents the first phase of a normal election when
    // Config.PreVote is true.
    enum CampaignType {
        campaignPreElection, campaignElection, campaignTransfer
    }

    final static byte[] campaignPreElection = CampaignType.campaignPreElection.name().getBytes();
    // campaignElection represents a normal (time-based) election (the second phase
    // of the election when Config.PreVote is true).
    final static byte[] campaignElection = CampaignType.campaignElection.name().getBytes();
    // campaignTransfer represents the type of leader transfer
    final static byte[] campaignTransfer = CampaignType.campaignTransfer.name().getBytes();

    final static int ReadOnlySafe = 0;
    final static int ReadOnlyLeaseBased = 1;
    // ErrProposalDropped is returned when the proposal is ignored by some cases,
    // so that the proposer can be notified and fail fast.
    final static Exception ErrProposalDropped = new Exception("raft proposal dropped");
}
