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

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@Builder
@Data
public class Raft {
    public static final long None = 0L;
    public static final long noLimit = Long.MAX_VALUE;

    long id;
    long term = None;
    long vote = None;

    List<ReadOnly.ReadState> readStates = new ArrayList<>();

    long raftState = None;

    RaftLog raftLog;

    int maxInflight;
    long maxMsgSize;

    Map<Long, Process> prs;
    Map<Long, Process> leaderPrs;

    long[] matchBuf;

    enum StateType {
        StateFollower,
        StateCandidate,
        StateLeader,
        StatePreCandidate,
        numStates
    }

    StateType state;

    boolean isLeader;

    long leader;

    Map<Long, Boolean> votes;
    ArrayList<Raftpb.Message> msgs;
    long lead = None;


    long leadTransferee;
    long pendingConfIndex;

    ReadOnly readOnly;

    int electionElapsed, heartbeatElapsed;

    boolean checkQuorum, preVote;

    int heartbeatTimeout, electionTimeout, randomizedElectionTimeout;

    boolean disableProposalForwarding;

    interface Tick {
        void run() throws Exception;
    }

    Tick tick;

    public interface StepFunc {
        void apply(Raft raft, Raftpb.Message m) throws Exception;
    }

    StepFunc step;
    RaftLog logger;


    final static int intn(int seed) {
        return ThreadLocalRandom.current().nextInt(seed);
    }

    public static Raft newRaft(Config c) throws Exception {
        c.validate();
        RaftLog raftLog = RaftLog.newLogWithSize(c.getStorage(), (RaftLogger) c.getLogger(), c.getMaxSizePerMsg());
        Storage.State state = c.getStorage().initialSate();
        Raftpb.HardState hs = state.getHardState();
        Raftpb.ConfState cs = state.getConfState();
        List<Long> peers = c.getPeers();
        List<Long> leaders = c.getLeaders();
        if (cs.getNodes().size() > 0 || cs.getLeaders().size() > 0) {
            if (peers.size() > 0 || leaders.size() > 0) {
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


        return r;
    }

    public List<Long> nodes() {
        List<Long> nodes = new ArrayList<>(this.prs.size());
        for (Long id : this.prs.keySet()) {
            nodes.add(id);
        }
        Collections.sort(nodes);
        return nodes;
    }

    public boolean hasLeader() {
        return this.lead != None;
    }

//    public Node.SoftState softState() {
//        return new Node.SoftState(lead, raftState);
//    }
//
//    public Raftpb.HardState hardState() {
//        return new Raftpb.HardState(term, vote, committed);
//    }

    private void stepLeader(Raft r, Raftpb.Message m) throws Exception {
        switch (m.getType()) {
            case MsgBeat:
                this.bcastHeartBeat();
                return;
            case MsgCheckQuorum: {
                if (!this.checkQuorumActive()) {
                    this.becomeFollower(this.term, None);
                }
                return;
            }
            case MsgProp: {
                if (m.getEntries().isEmpty()) {

                }
                if (!this.prs.containsKey(this.id)) {
                    throw ErrProposalDropped;
                }
                if (this.leadTransferee != None) {
                    throw ErrProposalDropped;
                }
                List<Raftpb.Entry> entries = m.getEntries();
                int size = entries.size();
                for (int i = 0; i < size; i++) {
                    Raftpb.Entry e = entries.get(i);
                    if (e.getType() == Raftpb.EntryType.ConfChange) {
                        entries.set(i, Raftpb.Entry.builder().type(Raftpb.EntryType.Normal).build());
                    } else {
                        this.pendingConfIndex = this.raftLog.lastIndex() + i + 1;
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
                        return;
                    }
                    switch (this.readOnly.option) {
                        case ReadOnlySafe:
                            this.readOnly.addRequest(this.raftLog.getCommitted(), m);
                            this.bcastHeartbeatWithCtx(m.getEntries().get(0).getData());
                            break;
                        case ReadOnlyLeaseBased:
                            long ri = this.raftLog.getCommitted();
                            if (m.getFrom() == None || m.getFrom() == this.id) {
                                this.readStates.add(ReadOnly.ReadState.builder()
                                        .index(this.raftLog.getCommitted())
                                        .requestCtx(m.getEntries().get(0).getData())
                                        .build());
                            } else {
                                this.send(Raftpb.Message.builder()
                                        .from(m.getFrom())
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
        Process pr = this.getProgress(m.getFrom());
        if (pr == null) {
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
                            this.sendAppend(m.getFrom());
                        }
                        while (r.maybeSendAppend(m.getFrom(), false)) ;

                        if (m.getFrom() == r.getLeadTransferee() && pr.getMatch() == this.raftLog.lastIndex()) {
                            this.sendTimeoutNow(m.getFrom());
                        }
                    }
                }
            }
            case MsgHeartbeatResp: {
                pr.setRecentActive(true);
                pr.resume();
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
                break;
            }
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
                }
                pr.pause();
                break;
            }
            case MsgUnreachable: {
                if (pr.getState() == Process.ProgressStateType.ProgressStateReplicate) {
                    pr.becomeProbe();
                }
                break;
            }
            case MsgTransferLeader: {
                if (pr.isLeader()) {
                    return;
                }
                long leadTransFeree = m.getFrom();
                long lastTransferee = this.getLeadTransferee();
                if (lastTransferee != None) {
                    if (lastTransferee == leadTransFeree) {
                        return;
                    }
                    this.abortLeaderTransfer();
                }
                if (lastTransferee == this.id) {
                    return;
                }
                this.electionElapsed = 0;
                this.leadTransferee = leadTransFeree;
                if (pr.getMatch() == this.getRaftLog().lastIndex()) {
                    this.sendTimeoutNow(leadTransFeree);
                } else {
                    this.sendAppend(leadTransFeree);
                }
                break;
            }
        }
        return;
    }

    private void stepCandidate(Raft r, Raftpb.Message m) throws Exception {
        Raftpb.MessageType myVoteRespType;
        if (this.getState() == StateType.StatePreCandidate) {
            myVoteRespType = Raftpb.MessageType.MsgPreVoteResp;
        } else {
            myVoteRespType = Raftpb.MessageType.MsgVoteResp;
        }
        switch (m.getType()) {
            case MsgProp:
                throw ErrProposalDropped;
            case MsgApp: {
                this.becomeFollower(m.getTerm(), m.getFrom());
                this.handleAppendEntries(m);
                break;
            }
            case MsgHeartbeat: {
                this.becomeFollower(m.getTerm(), m.getFrom());
                this.handleHeartbeat(m);
                break;
            }
            case MsgSnap: {
                this.becomeFollower(m.getTerm(), m.getFrom());
                this.handleSnapshot(m);
                break;
            }
            case MsgVoteResp: {
                int gr = this.poll(m.getFrom(), m.getType(), !m.isReject());
                int q = this.quorum();
                if (q == gr) {
                    this.campaign(CampaignType.campaignElection);
                } else if (q == (this.votes.size() - gr)) {
                    this.becomeFollower(this.getTerm(), None);
                }
                break;
            }
            case MsgTimeoutNow: {
                break;
            }
        }
        return;
    }

    private void stepFollower(Raft r, Raftpb.Message m) throws Exception {
        switch (m.getType()) {
            case MsgProp: {
                if (this.lead == None) {
                    throw ErrProposalDropped;
                } else if (this.isDisableProposalForwarding()) {
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
                    return;
                }
                m.setTo(this.lead);
                this.send(m);
                break;
            }
            case MsgTimeoutNow: {
                if (this.promotable()) {
                    this.campaign(CampaignType.campaignTransfer);
                } else {

                }
                break;
            }
            case MsgReadIndex: {
                if (this.lead == None) {
                    return;
                }
                m.setTo(this.lead);
                this.send(m);
                break;
            }
            case MsgReadIndexResp: {
                if (m.getEntries().size() != 1) {
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

    void becomeFollower(long term, long lead) {
        Raft r = this;
        r.step = this::stepFollower;
        r.reset(term);
        r.tick = this::tickElection;
        r.lead = lead;
        r.state = StateType.StateFollower;
    }

    void becomeCandidate() {
        if (this.state == StateType.StateLeader) {
            Util.panic("invalid transition [leader -> candidate]");
        }
        this.step = this::stepCandidate;
        this.reset(this.term + 1);
        this.tick = this::tickElection;
        this.vote = this.id;
        this.state = StateType.StateCandidate;
    }

    void becomePreCandidate() {
        if (this.state == StateType.StateLeader) {
            Util.panic("invalid transition [leader -> pre-candidate]");
        }
        this.step = this::stepCandidate;
        this.votes = new HashMap<>();
        this.state = StateType.StatePreCandidate;
    }

    void becomeLeader() throws Exception {
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
    }

    private void appendEntry(List<Raftpb.Entry> es) throws Exception {
        long li = this.raftLog.lastIndex();
        int size = es.size();
        for (int i = 0; i < size; i++) {
            Raftpb.Entry entry = es.get(i);
            entry.setTerm(this.term);
            entry.setIndex(li + 1 + i);
        }
        li = this.raftLog.append(es);
        this.getProgress(this.id).maybeUpdate(li);
        this.maybeCommit();
    }

    void campaign(CampaignType t) throws Exception {
        long term = 0L;
        Raftpb.MessageType voteMsg;
        if (t == CampaignType.campaignPreElection) {
            this.becomePreCandidate();
            voteMsg = Raftpb.MessageType.MsgPreVote;
            term = this.term + 1;
        } else {
            this.becomeCandidate();
            ;
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

    int poll(long id, Raftpb.MessageType t, boolean v) {
        if (v) {

        } else {

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

    private void send(Raftpb.Message m) {
        m.setFrom(this.id);
        switch (m.getType()) {
            case MsgVote:
            case MsgVoteResp:
            case MsgPreVote:
            case MsgPreVoteResp: {
                if (m.getTerm() == 0L) {
                    Util.panic("term should be set when sending %s", m.getType());
                }
            }
            break;
            default: {
                if (m.getTerm() != 0L) {
                    Util.panic("term should not be set when sending %s (was %d)", m.getType(), m.getTerm());
                }
                if (m.getType() != Raftpb.MessageType.MsgProp && m.getType() != Raftpb.MessageType.MsgReadIndex) {
                    m.setTerm(this.term);
                }
                this.msgs.add(m);
            }
            break;
        }
    }

    public void step(Raftpb.Message m) throws Exception {
        long term = m.getTerm();
        Raftpb.MessageType type = m.getType();
        if (term == 0L) {

        } else if (term > this.term) {
            if (type == Raftpb.MessageType.MsgVote || type == Raftpb.MessageType.MsgPreVote) {
                boolean force = Arrays.equals(m.getContext(), campaignTransfer);
                boolean inLease = this.checkQuorum && this.lead != None && this.electionElapsed < this.electionTimeout;
                if (!force && inLease) {
                    RaftLogger.raftLogger.infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
                            this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, m.type, m.from, m.logTerm, m.index, this.term, this.electionTimeout - this.electionElapsed);

                    return;
                }
            }
            if (type == Raftpb.MessageType.MsgPreVote) {

            } else if (type == Raftpb.MessageType.MsgPreVoteResp && !m.reject) {

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
                this.send(Raftpb.Message.builder().to(m.getFrom()).type(Raftpb.MessageType.MsgAppResp).build());
            } else if (type == Raftpb.MessageType.MsgPreVote) {
                RaftLogger.raftLogger.infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
                        this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, m.type, m.from, m.logTerm, m.index, m.term);
                this.send(Raftpb.Message.builder()
                        .to(m.getFrom())
                        .term(this.term)
                        .type(Raftpb.MessageType.MsgPreVoteResp)
                        .reject(true)
                        .build());
            } else {
                RaftLogger.raftLogger.infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
                        this.id, this.term, m.type, m.from, m.term);
            }
            return;
        }
        switch (type) {
            case MsgHup:
                if (this.state != StateType.StateLeader) {
                    List<Raftpb.Entry> ents = this.raftLog.slice(this.raftLog.applied + 1, this.raftLog.committed + 1, noLimit);
                    int n = numOfPendingConf(ents);
                    if (n != 0 && this.raftLog.committed > this.raftLog.applied) {
                        return;
                    }
                    if (this.preVote) {
                        this.campaign(CampaignType.campaignPreElection);
                    } else {
                        this.campaign(CampaignType.campaignElection);
                    }
                } else {

                }
                break;
            case MsgPreVote:
                if (this.isLeader) {
                    return;
                }
                boolean canVote =
                        (this.vote == m.getFrom()) ||
                                (this.vote == None && this.lead == None) ||
                                (m.getType() == Raftpb.MessageType.MsgPreVote && m.getTerm() > this.term);
                if (canVote && this.raftLog.isUpToDate(m.getIndex(), m.getLogTerm())) {
                    this.send(Raftpb.Message.builder().to(m.getFrom()).term(m.getTerm()).type(m.getType()).build());
                    if (m.getType() == Raftpb.MessageType.MsgVote) {
                        this.electionElapsed = 0;
                        this.vote = m.getFrom();
                    }
                } else {
                    this.send(Raftpb.Message.builder().to(m.getFrom()).term(this.term).type(m.type).reject(true).build());
                }
                break;
            default:
                this.step.apply(this, m);
                return;
        }

    }

    private Process getProgress(long id) {
        Process process = this.prs.get(id);
        if (process != null) return process;
        return this.leaderPrs.get(id);
    }

    public void forEachProcess(BiConsumer<Long, Process> f) {
        for (Map.Entry<Long, Process> entry : this.prs.entrySet()) {
            f.accept(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Long, Process> entry : this.leaderPrs.entrySet()) {
            f.accept(entry.getKey(), entry.getValue());
        }
    }

    public void bcastAppend() {
        this.forEachProcess((id, p) -> {
            if (id == this.getId()) {
                return;
            }
            this.sendAppend(id);
        });
    }

    public void sendHeartbeat(long to, byte[] ctx) {
        long commit = Math.min(this.getProgress(to).getMatch(), this.raftLog.getCommitted());
        this.send(Raftpb.Message.builder().to(to).type(Raftpb.MessageType.MsgHeartbeat).commit(commit).context(ctx).build());
    }

    public void bcastHeartbeatWithCtx(byte[] ctx) {
        this.forEachProcess((id, p) -> {
            if (id == this.id) {
                return;
            } else {
                this.sendHeartbeat(id, ctx);
            }
        });
    }

    public void bcastHeartBeat() {
        String lastCtx = this.getReadOnly().lastPendingRequestCtx();
        if (lastCtx.isEmpty()) {
            this.bcastHeartbeatWithCtx(null);
        } else {
            this.bcastHeartbeatWithCtx(lastCtx.getBytes());
        }
    }


    public boolean sendAppend(long to) {
        return this.maybeSendAppend(to, true);
    }

    public boolean maybeSendAppend(long to, boolean sendIfEmpty) {
        Process pr = this.getProgress(to);
        if (pr.isPaused()) {
            return false;
        }
        Raftpb.Message.MessageBuilder mBuilder = Raftpb.Message.builder().to(to);
        Exception errt = null, erre = null;
        long term = 0;
        List<Raftpb.Entry> ents =null ;
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
        if (Util.len(ents)==0 && !sendIfEmpty) {
            return false;
        }
        if (errt != null || erre != null) {
            if (!pr.isRecentActive()) {
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
                    return false;
                }
                Util.panic(err);
            } else {
                if (Util.isEmptySnap(snapshot)) {
                    Util.panic("need non-empty snapshot");
                }
                mBuilder.snapshot(snapshot);
                long sindex = snapshot.getMetadata().getIndex();
                long sterm = snapshot.getMetadata().getTerm();

                pr.becomeSnapshot(sindex);
            }
            m = mBuilder.build();
        } else {
            m = mBuilder.type(Raftpb.MessageType.MsgApp)
                    .index(pr.getNext() - 1)
                    .logTerm(term)
                    .entries(ents != null ? ents : Collections.emptyList())
                    .commit(this.raftLog.getCommitted()).build();
            if (!m.getEntries().isEmpty()) {
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

                }
            }

        }
        this.send(m);
        return true;
    }

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

    public void delProgress(long id) {
        this.prs.remove(this.prs, id);
        this.leaderPrs.remove(this.prs, id);
    }

    public void addNodeOrLearnerNode(long id, boolean isLeader) throws Exception {
        Process pr = this.getProgress(id);
        if (pr == null) {
            this.setProgress(id, 0, this.raftLog.lastIndex() + 1, isLeader);
        } else {
            if (isLeader && !pr.isLeader()) {
                return;
            }
            if (isLeader == pr.isLeader()) {
                return;
            }
            this.leaderPrs.remove(id);
            pr.setLeader(false);
            this.prs.put(id, pr);
        }
        if (this.id == id) {
            this.isLeader = isLeader;
        }
        pr = this.getProgress(id);
        pr.setRecentActive(true);
    }

    public boolean promotable() {
        return this.prs.containsKey(this.id);
    }

    public void addNode(long id) throws Exception {
        this.addNodeOrLearnerNode(id, false);
    }

    public void addLearner(long id) throws Exception {
        this.addNodeOrLearnerNode(id, true);
    }

    public int quorum() {
        return this.prs.size() / 2 + 1;
    }

    public void tickElection() throws Exception {
        this.electionElapsed++;
        if (this.promotable() && this.pastElectionTimeout()) {
            this.electionElapsed = 0;
            this.step(Raftpb.Message.builder().from(this.id).type(Raftpb.MessageType.MsgHup).build());
        }
    }

    public void tickHeartbeat() throws Exception {
        this.heartbeatElapsed++;
        this.electionElapsed++;
        if (this.electionElapsed >= this.electionTimeout) {
            this.electionElapsed = 0;
            if (this.checkQuorum) {
                this.send(Raftpb.Message.builder().from(this.id).type(Raftpb.MessageType.MsgCheckQuorum).build());
            }
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

    public boolean maybeCommit() throws Exception {
        int size = this.prs.size();
        if (this.matchBuf == null || this.matchBuf.length < size) {
            this.matchBuf = new long[size];
        }
        long[] mis = Arrays.copyOf(this.matchBuf, size);
        int idx = 0;
        for (Process p : this.prs.values()) {
            mis[idx] = p.getMatch();
            idx++;
        }
        Arrays.sort(mis);
        long mci = mis[mis.length - this.quorum()];
        return this.raftLog.matchCommit(mci, this.getTerm());
    }

    public void restoreNode(List<Long> nodes, boolean isLeader) throws Exception {
        for (long n : nodes) {
            long match = 0;
            long next = this.raftLog.lastIndex() + 1;
            if (n == this.id) {
                match = next - 1;
                this.isLeader = isLeader;
            }
            this.setProgress(n, match, next, isLeader);
        }

    }

    public void reset(long term) {
        if (this.term != term) {
            this.term = term;
            this.vote = None;
        }
        this.lead = None;

        this.electionElapsed = 0;
        this.heartbeatElapsed = 0;

        this.randomizedElectionTimeout();

        this.abortLeaderTransfer();

        this.votes = new HashMap<>();
        this.forEachProcess((id,process)->{
            try {

                process.next = this.raftLog.lastIndex() + 1;
                process.setIns(Inflights.newInflights(this.getMaxInflight()));

                if (id == this.id){
                    process.match = this.raftLog.lastIndex();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    public boolean checkQuorumActive() {
        AtomicInteger act = new AtomicInteger(0);
        this.forEachProcess((id, pr) -> {
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

    public void loadState(Raftpb.HardState state) throws Exception {
        if (state.getCommit() < this.raftLog.getCommitted() ||
                state.getCommit() > this.raftLog.lastIndex()) {

        }
        this.raftLog.setCommitted(state.getCommit());
        this.term = state.getTerm();
        this.vote = state.getVote();
    }

    private void setTimeoutNow(long to) {
        this.send(Raftpb.Message.builder().to(to).type(Raftpb.MessageType.MsgTimeoutNow).build());
    }

    private boolean pastElectionTimeout() {
        return this.electionElapsed >= this.randomizedElectionTimeout;
    }

    private void randomizedElectionTimeout() {
        this.randomizedElectionTimeout =
                this.electionTimeout +
                        ThreadLocalRandom.current().nextInt(this.electionTimeout + 2);

    }

    public void removeNode(long id) throws Exception {
        this.delProgress(id);
        if (this.prs.isEmpty() && this.leaderPrs.isEmpty()) {
            return;
        }
        if (this.maybeCommit()) {
            this.bcastAppend();
        }
        if (this.state == StateType.StateLeader && this.leadTransferee == id) {
            this.abortLeaderTransfer();
        }
    }

    public void handleAppendEntries(Raftpb.Message m) throws Exception {
        if (m.getIndex() < this.raftLog.committed) {
            this.send(Raftpb.Message.builder().to(m.getFrom()).type(Raftpb.MessageType.MsgAppResp).index(this.raftLog.committed).build());
            return;
        }
        RaftLog.MaybeAppendResult mlastIndex = this.raftLog.maybeAppend(m.getIndex(), m.getTerm(), m.getCommit(), m.getEntries());
        if (mlastIndex.ok) {
            this.send(Raftpb.Message.builder().to(m.getFrom()).type(Raftpb.MessageType.MsgAppResp).index(mlastIndex.lastnewi).build());
        } else {
            this.send(Raftpb.Message.builder().to(m.getFrom()).type(Raftpb.MessageType.MsgAppResp).reject(true).rejectHint(this.raftLog.lastIndex()).build());
        }
    }

    public void handleHeartbeat(Raftpb.Message m) throws Exception {
        this.raftLog.commitTo(m.getCommit());
        this.send(Raftpb.Message.builder()
                .to(m.getFrom())
                .type(Raftpb.MessageType.MsgHeartbeatResp)
                .context(m.getContext())
                .build());
    }

    public boolean restore(Raftpb.Snapshot s) throws Exception {
        if (s.getMetadata().getIndex() <= this.raftLog.getCommitted()) {
            return false;
        }
        if (this.raftLog.matchTerm(s.getMetadata().getIndex(), s.getMetadata().getTerm())) {
            RaftLogger.raftLogger.infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
                    this.id, this.raftLog.committed, this.raftLog.lastIndex(), this.raftLog.lastTerm(), s.getMetadata().getIndex(), s.getMetadata().getTerm());
            this.raftLog.commitTo(s.getMetadata().getIndex());
            return false;
        }
        if (!this.isLeader()) {
            List<Long> leaders = s.getMetadata().getConfState().getLeaders();
            for (long id : leaders) {
                if (id == this.id) {
                    RaftLogger.raftLogger.errorf("%x can't become learner when restores snapshot [index: %d, term: %d]", this.id, s.getMetadata().getIndex(), s.getMetadata().getTerm());
                    return false;
                }
            }
        }
        this.raftLog.restore(s);
        this.prs = new HashMap<>();
        this.leaderPrs = new HashMap<>();
        this.restoreNode(s.getMetadata().getConfState().getNodes(), false);
        this.restoreNode(s.getMetadata().getConfState().getLeaders(), true);
        return true;
    }

    public void restore(long[] nodes, boolean isLeader) throws Exception {
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

    public void handleSnapshot(Raftpb.Message m) throws Exception {
        long index = m.getSnapshot().getMetadata().getIndex();
        long term = m.getSnapshot().getMetadata().getTerm();
        if (this.restore(m.getSnapshot())) {
            this.send(Raftpb.Message.builder()
                    .to(m.getFrom())
                    .type(Raftpb.MessageType.MsgAppResp)
                    .index(this.raftLog.lastIndex())
                    .build());
        } else {
            this.send(Raftpb.Message.builder()
                    .to(m.getFrom())
                    .type(Raftpb.MessageType.MsgAppResp)
                    .index(this.raftLog.getCommitted())
                    .build());
        }
    }


    public void sendTimeoutNow(long to) {
        this.send(Raftpb.Message.builder().to(to).type(Raftpb.MessageType.MsgTimeoutNow).build());
    }

    public void abortLeaderTransfer() {
        this.leadTransferee = None;
    }

    public int numOfPendingConf(List<Raftpb.Entry> ents) {
        int n = 0;
        for (Raftpb.Entry ent : ents) {
            if (ent.getType() == Raftpb.EntryType.ConfChange) {
                ++n;
            }
        }
        return n;
    }

    enum CampaignType {
        campaignPreElection, campaignElection, campaignTransfer
    }

    final static byte[] campaignPreElection = CampaignType.campaignPreElection.name().getBytes();
    final static byte[] campaignElection = CampaignType.campaignElection.name().getBytes();
    final static byte[] campaignTransfer = CampaignType.campaignTransfer.name().getBytes();

    final static int ReadOnlySafe = 0;
    final static int ReadOnlyLeaseBased = 1;
    final static Exception ErrProposalDropped = new Exception("raft proposal dropped");
}
