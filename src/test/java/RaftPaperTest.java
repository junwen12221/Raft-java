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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * cjw
 */
public class RaftPaperTest {

    /**
     * cjw
     * // testUpdateTermFromMessage tests that if one server’s current term is
     * // smaller than the other’s, then it updates its current term to the larger
     * // value. If a candidate or leader discovers that its term is out of date,
     * // it immediately reverts to follower state.
     * // Reference: section 5.1
     *
     * @param stateType
     * @throws Exception
     * @finished
     */
    public static void testUpdateTermFromMessage(Raft.StateType stateType) throws Exception {
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, 1, MemoryStorage.newMemoryStorage());
        switch (stateType) {
            case StateFollower:
                r.becomeFollower(1, 2);
                break;
            case StateCandidate:
                r.becomeFollower(1, 2);
                break;
            case StateLeader:
                r.becomeCandidate();
                r.becomeLeader();
                break;
        }
        r.step(Raftpb.Message.builder().type(Raftpb.MessageType.MsgApp).term(2).build());
        if (r.getTerm() != 2) {
            Assert.fail(String.format("term = %d, want %d", r.term, 2));
        }
        if (r.getState() != Raft.StateType.StateFollower) {
            Assert.fail(String.format("state = %s, want %s", r.state.toString(), Raft.StateType.StateFollower.toString()));
        }
    }

    @Test
    public void testFollowerUpdateTermFromMessage() throws Exception {
        testUpdateTermFromMessage(Raft.StateType.StateFollower);
    }

    @Test
    public void testCandidateUpdateTermFromMessage() throws Exception {
        testUpdateTermFromMessage(Raft.StateType.StateCandidate);
    }

    @Test
    public void testLeaderUpdateTermFromMessage() throws Exception {
        testUpdateTermFromMessage(Raft.StateType.StateLeader);
    }

    /**
     * // TestRejectStaleTermMessage tests that if a server receives a request with
     * // a stale term number, it rejects the request.
     * // Our implementation ignores the request instead.
     * // Reference: section 5.1
     *
     * @finished
     */
    @Test
    public void testRejectStaleTermMessage() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);

        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, 1, MemoryStorage.newMemoryStorage());
        r.step = new Raft.StepFunc() {
            @Override
            public void apply(Raft raft, Raftpb.Message m) {
                called.set(true);
            }
        };
        r.loadState(Raftpb.HardState.builder().term(2).build());

        if (called.get()) {
            Assert.fail(String.format("stepFunc called = %s, want %b", called.toString(), false));
        }
    }

    // TestStartAsFollower tests that when servers start up, they begin as followers.
    // Reference: section 5.2
    @Test
    public void testStartAsFollower() throws Exception {
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, 1, MemoryStorage.newMemoryStorage());
        if (r.getState() != Raft.StateType.StateFollower) {
            RaftTestUtil.errorf("state = %s, want %s", r.state, Raft.StateType.StateFollower);
        }
    }

    // TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
    // it will send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries as
    // heartbeat to all followers.
    // Reference: section 5.2
    @Test
    public void testLeaderBcastBeat() throws Exception {
        // heartbeat interval
        int hi = 1;
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, hi, MemoryStorage.newMemoryStorage());
        r.becomeCandidate();
        r.becomeLeader();
        for (int i = 0; i < 10; i++) {
            r.appendEntry(Arrays.asList(Raftpb.Entry.builder().index(i + 1).build()));
        }
        for (int i = 0; i < hi; i++) {
            r.tick.run();
        }
        List<Raftpb.Message> msgs = RaftTestUtil.readMessages(r);
        msgs.sort(Comparator.comparing(Raftpb.Message::toString));
        List<Raftpb.Message> wmsgs = Arrays.asList(
                Raftpb.Message.builder().from(1).to(2).term(1).type(Raftpb.MessageType.MsgHeartbeat).build(),
                Raftpb.Message.builder().from(1).to(3).term(1).type(Raftpb.MessageType.MsgHeartbeat).build()
        );
        if (!msgs.equals(wmsgs)) {
            RaftTestUtil.errorf("msgs = %s, want %s", msgs.toString(), wmsgs.toString());
        }
    }

    @Test
    public void testFollowerStartElection() throws Exception {
        testNonleaderStartElection(Raft.StateType.StateFollower);
    }

    @Test
    public void testCandidateStartNewElection() throws Exception {
        testNonleaderStartElection(Raft.StateType.StateCandidate);
    }

    // testNonleaderStartElection tests that if a follower receives no communication
    // over election timeout, it begins an election to choose a new leader. It
    // increments its current term and transitions to candidate state. It then
    // votes for itself and issues RequestVote RPCs in parallel to each of the
    // other servers in the cluster.
    // Reference: section 5.2
    // Also if a candidate fails to obtain a majority, it will time out and
    // start a new election by incrementing its term and initiating another
    // round of RequestVote RPCs.
    // Reference: section 5.2
    public void testNonleaderStartElection(Raft.StateType state) throws Exception {
        // election timeout
        int et = 10;
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), et, 1, MemoryStorage.newMemoryStorage());
        switch (state) {
            case StateFollower:
                r.becomeFollower(1, 2);
                break;
            case StateCandidate:
                r.becomeCandidate();
                break;
        }
        for (int i = 1; i < 2 * et; i++) {
            r.tick.run();
        }
        if (r.getTerm() != 2) {
            RaftTestUtil.errorf("term = %d, want 2", r.term);
        }
        if (r.getState() != Raft.StateType.StateCandidate) {
            RaftTestUtil.errorf("state = %s, want %s", r.state, Raft.StateType.StateCandidate);
        }
        if (!r.getVotes().get(r.getId())) {
            RaftTestUtil.errorf("vote for self = false, want true");
        }
        List<Raftpb.Message> msgs = RaftTestUtil.readMessages(r);
        msgs.sort(Comparator.comparing(Raftpb.Message::toString));
        List<Raftpb.Message> wmsgs = Arrays.asList(
                Raftpb.Message.builder().from(1).to(2).term(2).type(Raftpb.MessageType.MsgVote).build(),
                Raftpb.Message.builder().from(1).to(3).term(2).type(Raftpb.MessageType.MsgVote).build()
        );
        if (!msgs.equals(wmsgs)) {
            RaftTestUtil.errorf("msgs = %s, want %s", msgs.toString(), wmsgs.toString());
        }
    }

    LeaderElectionInOneRoundRPCCase Case(
            int size,
            Map<Long, Boolean> votes,
            Raft.StateType state) {
        return new LeaderElectionInOneRoundRPCCase(size, votes, state);
    }

    Map<Long, Boolean> MapOf(Object... list) {
        HashMap<Long, Boolean> map = new HashMap<>();
        for (int i = 0; i < list.length; i += 2) {
            map.put(((Integer) list[i]).longValue(), (Boolean) list[i + 1]);
        }
        return map;
    }

    // TestLeaderElectionInOneRoundRPC tests all cases that may happen in
    // leader election during one round of RequestVote RPC:
    // a) it wins the election
    // b) it loses the election
    // c) it is unclear about the result
    // Reference: section 5.2
    @Test
    public void testLeaderElectionInOneRoundRPC() throws Exception {
        List<LeaderElectionInOneRoundRPCCase> tests = Arrays.asList(
                // win the election when receiving votes from a majority of the servers
                Case(1, new HashMap<>(), Raft.StateType.StateLeader),
                Case(3, MapOf(2, true, 3, true), Raft.StateType.StateLeader),
                Case(3, MapOf(2, true), Raft.StateType.StateLeader),
                Case(5, MapOf(2, true, 3, true, 4, true, 5, true), Raft.StateType.StateLeader),
                Case(5, MapOf(2, true, 3, true, 4, true), Raft.StateType.StateLeader),
                Case(5, MapOf(2, true, 3, true), Raft.StateType.StateLeader),

                // return to follower state if it receives vote denial from a majority
                Case(3, MapOf(2, false, 3, false), Raft.StateType.StateFollower),
                Case(5, MapOf(2, false, 3, false, 4, false, 5, false), Raft.StateType.StateFollower),
                Case(5, MapOf(2, true, 3, false, 4, false, 5, false), Raft.StateType.StateFollower),

                // stay in candidate if it does not obtain the majority
                Case(3, MapOf(), Raft.StateType.StateCandidate),
                Case(5, MapOf(2, true), Raft.StateType.StateCandidate),
                Case(5, MapOf(2, false, 3, false), Raft.StateType.StateCandidate),
                Case(5, MapOf(), Raft.StateType.StateCandidate)
        );
        int i = 0;
        for (LeaderElectionInOneRoundRPCCase test : tests) {

            Raft r = RaftTestUtil.newTestRaft(1, RaftTestUtil.idsBySize(test.size), 10, 1, MemoryStorage.newMemoryStorage());

            r.step(Raftpb.Message.builder().from(1).to(1).type(Raftpb.MessageType.MsgHup).build());

            for (Map.Entry<Long, Boolean> entry : test.votes.entrySet()) {
                Long id = entry.getKey();
                Boolean vote = entry.getValue();
                r.step(Raftpb.Message.builder().from(id).to(1).term(r.getTerm()).type(Raftpb.MessageType.MsgVoteResp).reject(!vote).build());
            }
            if (r.state != test.state) {
                RaftTestUtil.errorf("#%d: state = %s, want %s", i, r.state, test.state);
            }
            long g = r.getTerm();
            if (g != 1) {
                RaftTestUtil.errorf("#%d: state = %s, want %s", i, g, 1);
            }
            ++i;
        }
    }

    FollowerVoteCase Case(
            long vote,
            long nvote,
            boolean wreject
    ) {
        return new FollowerVoteCase(vote, nvote, wreject);
    }

    @Test
    public void testFollowerVote() throws Exception {

        List<FollowerVoteCase> tests = Arrays.asList(
                Case(Raft.None, 1, false),
                Case(Raft.None, 2, false),
                Case(1, 1, false),
                Case(2, 2, false),
                Case(1, 2, true),
                Case(2, 1, true)
        );

        int i = 0;
        for (FollowerVoteCase tt : tests) {
            Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, 1, MemoryStorage.newMemoryStorage());
            r.loadState(Raftpb.HardState.builder().term(1).vote(tt.vote).build());
            r.step(Raftpb.Message.builder().from(tt.nvote).to(1).term(1).type(Raftpb.MessageType.MsgVote).build());

            List<Raftpb.Message> msgs = RaftTestUtil.readMessages(r);
            List<Raftpb.Message> wmsgs = Arrays.asList(Raftpb.Message.builder().from(1).to(tt.nvote).type(Raftpb.MessageType.MsgVoteResp).reject(tt.wreject).build());
            if (msgs.equals(wmsgs)) {
                RaftTestUtil.errorf("#%d: msgs = %s, want %s", i, msgs.toString(), wmsgs.toString());
            }
        }
    }

    // TestCandidateFallback tests that while waiting for votes,
    // if a candidate receives an AppendEntries RPC from another server claiming
    // to be leader whose term is at least as large as the candidate's current term,
    // it recognizes the leader as legitimate and returns to follower state.
    // Reference: section 5.2
    @Test
    public void testCandidateFallback() throws Exception {
        Raftpb.Message m1 = Raftpb.Message.builder().from(2).to(1).term(1).type(Raftpb.MessageType.MsgApp).build();
        Raftpb.Message m2 = Raftpb.Message.builder().from(2).to(1).term(2).type(Raftpb.MessageType.MsgApp).build();
        List<Raftpb.Message> tests = Arrays.asList(m1, m2);

        int i = 0;
        for (Raftpb.Message tt : tests) {
            Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, 1, MemoryStorage.newMemoryStorage());
            r.step(Raftpb.Message.builder().from(1).to(1).type(Raftpb.MessageType.MsgHup).build());

            if (r.state != Raft.StateType.StateCandidate) {
                Assert.fail(String.format("unexpected state = %s, want %s", r.state, Raft.StateType.StateCandidate));
            }

            r.step(tt);

            if (r.getState() != Raft.StateType.StateFollower) {
                RaftTestUtil.errorf("#%d: state = %s, want %s", i, r.getState(), Raft.StateType.StateFollower);
            }
            if (r.getTerm() != tt.getTerm()) {
                RaftTestUtil.errorf("#%d: term = %d, want %d", i, r.getTerm(), tt.term);
            }
        }
    }

    @Test
    public void testFollowerElectionTimeoutRandomized() throws Exception {
        testNonleaderElectionTimeoutRandomized(Raft.StateType.StateFollower);
    }

    @Test
    public void testCandidateElectionTimeoutRandomized() throws Exception {
        testNonleaderElectionTimeoutRandomized(Raft.StateType.StateCandidate);
    }

    // testNonleaderElectionTimeoutRandomized tests that election timeout for
    // follower or candidate is randomized.
    // Reference: section 5.2
    public void testNonleaderElectionTimeoutRandomized(Raft.StateType state) throws Exception {
        int et = 10;
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), et, 1, MemoryStorage.newMemoryStorage());
        Map<Integer, Boolean> timeouts = new HashMap<>();
        for (int round = 0; round < 50 * et; round++) {
            switch (state) {
                case StateFollower:
                    r.becomeFollower(r.getTerm() + 1, 2);
                    break;
                case StateCandidate:
                    r.becomeCandidate();
                    break;
            }

            int time = 0;
            while (0 == Util.len(RaftTestUtil.readMessages(r))) {
                r.tick.run();
                time++;
            }
            timeouts.put(time, true);
        }
        for (int d = et + 1; d < 2 * et; d++) {
            if (!timeouts.get(d)) {
                RaftTestUtil.errorf("timeout in %d ticks should happen", d);
            }
        }
    }

    @Test
    public void testFollowersElectionTimeoutNonconflict() throws Exception {
        testNonleadersElectionTimeoutNonconflict(Raft.StateType.StateFollower);
    }

    @Test
    public void testCandidatesElectionTimeoutNonconflict() throws Exception {
        testNonleadersElectionTimeoutNonconflict(Raft.StateType.StateCandidate);
    }

    // testNonleadersElectionTimeoutNonconflict tests that in most cases only a
    // single server(follower or candidate) will time out, which reduces the
    // likelihood of split vote in the new election.
    // Reference: section 5.2
    public void testNonleadersElectionTimeoutNonconflict(Raft.StateType state) throws Exception {
        int et = 10;
        int size = 5;
        Raft[] rs = new Raft[size];
        List<Long> ids = RaftTestUtil.idsBySize(size);
        for (int i = 0; i < size; i++) {
            rs[i] = RaftTestUtil.newTestRaft(ids.get(i), ids, et, 1, MemoryStorage.newMemoryStorage());
        }
        int conflicts = 0;
        for (int round = 0; round < 1000; round++) {
            for (Raft r : rs) {
                switch (state) {
                    case StateFollower:
                        r.becomeFollower(r.getTerm() + 1, Raft.None);
                        break;
                    case StateCandidate:
                        r.becomeCandidate();
                        break;
                }
            }

            int timeoutNum = 0;
            while (timeoutNum == 0) {
                for (Raft r : rs) {
                    r.tick.run();
                    if (Util.len(RaftTestUtil.readMessages(r)) > 0) {
                        timeoutNum++;
                    }
                }
            }
            // several rafts time out at the same tick
            if (timeoutNum > 1) {
                conflicts++;
            }
        }
        double g = conflicts * 1.0 / 1000;
        if (g > 0.3) {
            Assert.fail(String.format("probability of conflicts = %f, want <= 0.3", g));
        }
    }

    // TestLeaderStartReplication tests that when receiving client proposals,
    // the leader appends the proposal to its log as a new entry, then issues
    // AppendEntries RPCs in parallel to each of the other servers to replicate
    // the entry. Also, when sending an AppendEntries RPC, the leader includes
    // the index and term of the entry in its log that immediately precedes
    // the new entries.
    // Also, it writes the new entry into stable storage.
    // Reference: section 5.3
    @Test
    public void testLeaderStartReplication() throws Exception {
        MemoryStorage s = MemoryStorage.newMemoryStorage();
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        commitNoopEntry(r, s);
        long li = r.raftLog.lastIndex();

        List<Raftpb.Entry> ents = Arrays.asList(Raftpb.Entry.builder().data("some data".getBytes()).build());
        r.step(Raftpb.Message.builder().from(1).to(1).type(Raftpb.MessageType.MsgProp).entries(ents).build());

        long g = r.raftLog.lastIndex();
        if (g != li + 1) {
            RaftTestUtil.errorf("lastIndex = %d, want %d", g, li + 1);
        }
        g = r.raftLog.committed;
        if (g != li) {
            RaftTestUtil.errorf("committed = %d, want %d", g, li);
        }
        List<Raftpb.Message> msgs = RaftTestUtil.readMessages(r);
        msgs.sort(Comparator.comparing(Raftpb.Message::toString));

        List<Raftpb.Entry> wents = Arrays.asList(Raftpb.Entry.builder().index(li + 1).term(1).data("some data".getBytes()).build());

        Raftpb.Message c1 = Raftpb.Message.builder()
                .from(1).to(2).term(1).type(Raftpb.MessageType.MsgApp).index(li).logTerm(1).entries(wents).commit(1).build();
        Raftpb.Message c2 = Raftpb.Message.builder()
                .from(1).to(3).term(1).type(Raftpb.MessageType.MsgApp).index(li).logTerm(1).entries(wents).commit(1).build();

        List<Raftpb.Message> wmsgs = Arrays.asList(c1, c2);
        if (!msgs.equals(wmsgs)) {
            Assert.fail(String.format("msgs = %s, want %s", msgs.toString(), wmsgs.toString()));
        }

        if (!r.raftLog.unstableEntries().equals(wents)) {
            RaftTestUtil.errorf("ents = %s, want %s", r.raftLog.unstableEntries(), wents);
        }
    }

    // TestLeaderCommitEntry tests that when the entry has been safely replicated,
    // the leader gives out the applied entries, which can be applied to its state
    // machine.
    // Also, the leader keeps track of the highest index it knows to be committed,
    // and it includes that index in future AppendEntries RPCs so that the other
    // servers eventually find out.
    // Reference: section 5.3
    @Test
    public void testLeaderCommitEntry() throws Exception {
        MemoryStorage s = MemoryStorage.newMemoryStorage();
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L, 3L), 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        commitNoopEntry(r, s);
        long li = r.raftLog.lastIndex();

        List<Raftpb.Entry> ents = Arrays.asList(Raftpb.Entry.builder().data("some data".getBytes()).build());
        r.step(Raftpb.Message.builder().from(1).to(1).type(Raftpb.MessageType.MsgProp).entries(ents).build());

        List<Raftpb.Message> messages = RaftTestUtil.readMessages(r);
        for (Raftpb.Message m : messages) {
            r.step(acceptAndReply(m));
        }

        long g = 0;
        g = r.raftLog.committed;
        if (g != li + 1) {
            RaftTestUtil.errorf("committed = %d, want %d", g, li + 1);
        }
        List<Raftpb.Entry> wents = Arrays.asList(Raftpb.Entry.builder().index(li + 1).term(1).data("some data".getBytes()).build());
        List<Raftpb.Entry> entries = r.raftLog.nextEnts();
        if (!entries.equals(wents)) {
            RaftTestUtil.errorf("nextEnts = %+v, want %s", entries, wents);
        }
        List<Raftpb.Message> msgs = RaftTestUtil.readMessages(r);
        msgs.sort(Comparator.comparing(Raftpb.Message::toString));
        int i = 0;
        for (Raftpb.Message m : msgs) {
            long w = i + 2;
            if (m.to != w) {
                RaftTestUtil.errorf("to = %x, want %x", m.to, w);
            }
            if (m.getType() != Raftpb.MessageType.MsgApp) {
                RaftTestUtil.errorf("type = %v, want %v", m.type, Raftpb.MessageType.MsgApp);
            }
            if (m.commit != li + 1) {
                RaftTestUtil.errorf("commit = %d, want %d", m.commit, li + 1);
            }
            ++i;
        }
    }

    LeaderAcknowledgeCommitCase Case(
            int size,
            Map<Long, Boolean> acceptors,
            boolean wack
    ) {
        return new LeaderAcknowledgeCommitCase(size, acceptors, wack);
    }

    // TestLeaderAcknowledgeCommit tests that a log entry is committed once the
    // leader that created the entry has replicated it on a majority of the servers.
    // Reference: section 5.3
    @Test
    public void testLeaderAcknowledgeCommit() throws Exception {
        List<LeaderAcknowledgeCommitCase> tests = Arrays.asList(
                Case(1, null, true),
                Case(3, null, false),
                Case(3, MapOf(2, true), true),
                Case(3, MapOf(2, true, 3, true), true),
                Case(5, null, false),

                Case(5, MapOf(2, true), false),
                Case(5, MapOf(2, true, 3, true), true),
                Case(5, MapOf(2, true, 3, true, 4, true), true),
                Case(5, MapOf(2, true, 3, true, 4, true, 5, true), true)
        );
        int i = 0;
        for (LeaderAcknowledgeCommitCase tt : tests) {
            MemoryStorage s = MemoryStorage.newMemoryStorage();
            Raft r = RaftTestUtil.newTestRaft(1, RaftTestUtil.idsBySize(tt.size), 10, 1, s);
            r.becomeCandidate();
            r.becomeLeader();
            commitNoopEntry(r, s);
            long li = r.raftLog.lastIndex();
            r.step(Raftpb.Message.builder().from(1).to(1).type(Raftpb.MessageType.MsgProp).entries(Arrays.asList(Raftpb.Entry.builder()
                    .data("some data".getBytes())
                    .build())).build());

            for (Raftpb.Message m : RaftTestUtil.readMessages(r)) {
                if (tt.acceptors != null && tt.acceptors.containsKey(m.to)) {
                    r.step(acceptAndReply(m));
                }
            }
            boolean g = r.raftLog.committed > li;
            if (g != tt.wack) {
                RaftTestUtil.errorf("#%d: ack commit = %b, want %b", i, g, tt.wack);
            }
            ++i;
        }


    }

    @AllArgsConstructor
    @Builder
    @Value
    static class LeaderAcknowledgeCommitCase {
        int size;
        Map<Long, Boolean> acceptors;
        boolean wack;
    }

    public void commitNoopEntry(Raft r, MemoryStorage s) throws Exception {
        if (r.state != Raft.StateType.StateLeader) {
            Util.panic("it should only be used when it is the leader");
        }
        r.bcastAppend();
        List<Raftpb.Message> msgs = RaftTestUtil.readMessages(r);

        for (Raftpb.Message msg : msgs) {
            if (msg.getType() != Raftpb.MessageType.MsgApp || Util.len(msg.getEntries()) != 1 || msg.getEntries().get(0).getData() != null) {
                Util.panic("not a message to append noop entry");
            }
            r.step(acceptAndReply(msg));
        }
        RaftTestUtil.readMessages(r);
        s.append(r.getRaftLog().unstableEntries());
        r.raftLog.appliesTo(r.raftLog.committed);
        r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm());

    }

    private Raftpb.Message acceptAndReply(Raftpb.Message msg) {
        if (msg.getType() != Raftpb.MessageType.MsgApp) {
            Util.panic("type should be MsgApp");
        }
        return Raftpb.Message.builder().from(msg.to).to(msg.from).type(Raftpb.MessageType.MsgAppResp)
                .index(msg.index + Util.len(msg.getEntries())).build();
    }

    @AllArgsConstructor
    @Builder
    @Value
    static class LeaderElectionInOneRoundRPCCase {
        int size;
        Map<Long, Boolean> votes;
        Raft.StateType state;
    }

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2

    @AllArgsConstructor
    @Value
    @Builder
    static class FollowerVoteCase {
        long vote;
        long nvote;
        boolean wreject;
    }


}
