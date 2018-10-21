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
import org.junit.Assert;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/***
 * cjw
 */
public class RaftTestUtil {

    public static void errorf(String tmp,Object... args){
        Assert.fail(String.format(tmp,args));
    }
    public static Config newTestConfig(long id, List<Long> peer, int election, int heatbeat, Storage storage) {
        return Config.builder()
                .leaders(new ArrayList<>())
                .id(id)
                .peers(peer)
                .electionTick(election)
                .heartbeatTick(heatbeat)
                .storage(storage)
                .maxSizePerMsg(Raft.noLimit)
                .maxInflightMsgs(256)
                .build();
    }

    interface StateMachine {
        void step(Raftpb.Message m) throws Exception;

        List<Raftpb.Entry> readMessage();
    }

    public static List<Long> idsBySize(int size) {
        long[] ids = new long[size];
        for (int i = 0; i < size; i++) {
            ids[i] = 1L + i;
        }
        return Arrays.stream(ids).boxed().collect(Collectors.toList());
    }

    public static List<Raftpb.Message> readMessages(Raft r) {
        ArrayList<Raftpb.Message> msgs = r.getMsgs();
        r.setMsgs(new ArrayList<>());
        return msgs;
    }

    public static class BlackHole implements StateMachine {
        @Override
        public void step(Raftpb.Message m) {

        }

        @Override
        public List<Raftpb.Entry> readMessage() {
            return null;
        }
    }

    public final static BlackHole nopStepper = new BlackHole();

    public Raft entsWithConfig(Consumer<Config> configFunc, long... terms) throws Exception {
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        for (int i = 0; i < terms.length; i++) {
            long term = terms[i];
            storage.append(Collections.singletonList(Raftpb.Entry.builder()
                    .index(i + 1)
                    .term(term)
                    .build()));
        }
        Config cfg = newTestConfig(1, Arrays.asList(0L), 5, 1, storage);
        if (configFunc != null) {
            configFunc.accept(cfg);
        }
        Raft sm = Raft.newRaft(cfg);
        sm.reset(terms[terms.length - 1]);
        return sm;
    }

    public Raft votedWIthConfig(Consumer<Config> configFunc, long vote, long term) throws Exception {
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.setHardState(Raftpb.HardState.builder().vote(vote).term(term).build());
        Config cfg = newTestConfig(1, Arrays.asList(), 5, 1, storage);
        if (configFunc != null) {
            configFunc.accept(cfg);
        }
        Raft sm = Raft.newRaft(cfg);
        sm.reset(term);
        return sm;
    }

    @Data
    @Builder
    public static class Network {
        Map<Long, Object> peers;
        Map<Long, MemoryStorage> storage;
        Map<Connem, Double> dropm;
        Map<Raftpb.MessageType, Boolean> ignorem;
        Function<Raftpb.Message, Boolean> msgHook;

        public Network newNetwork(StateMachine... peers) throws Exception {
            return newNetworkWithConfig(null, peers);
        }

        public Network newNetworkWithConfig(Consumer<Config> configFunc, Object... peers) throws Exception {
            int size = peers.length;
            List<Long> peerAddrs = idsBySize(size);

            Map<Long, Object> npeers = new HashMap<>(size);
            Map<Long, MemoryStorage> nstoage = new HashMap<>(size);

            for (int j = 0; j < size; j++) {
                long id = peerAddrs.get(j);
                Object p = peers[j];
                if (p == null) {
                    nstoage.put(id, MemoryStorage.newMemoryStorage());
                    Config cfg = newTestConfig(id, peerAddrs, 10, 1, nstoage.get(id));
                    if (configFunc != null) {
                        configFunc.accept(cfg);
                    }
                    Raft sm = Raft.newRaft(cfg);
                    npeers.put(id, sm);
                } else if (p instanceof BlackHole) {
                    npeers.put(id, p);
                } else if (p instanceof Raft) {
                    Map<Long, Boolean> leaders = new HashMap<>();
                    Raft v = (Raft) p;
                    v.getLeaderPrs().keySet().forEach(k -> leaders.put(k, Boolean.TRUE));
                    v.setId(id);
                    v.prs = new HashMap<>();
                    v.leaderPrs = new HashMap<>();

                    for (int i = 0; i < size; i++) {
                        Boolean ok = leaders.get(peerAddrs.get(i));
                        if (ok) {
                            v.getLeaderPrs().put(peerAddrs.get(i), Process.builder().isLeader(true).build());
                        } else {
                            v.prs.put(peerAddrs.get(i), Process.builder().build());
                        }
                    }
                    v.reset(v.getTerm());
                    npeers.put(id, v);
                } else {
                    Util.panic("unexpected state machine type: %T", p);
                }

            }
            return Network.builder()
                    .peers(npeers)
                    .storage(nstoage)
                    .dropm(new HashMap<>())
                    .ignorem(new HashMap<>())
                    .build();
        }

        public void preVoteConfig(Config c) {
            c.setPreVote(true);
        }

        public void send(Raftpb.Message... msgs) {
            List<Raftpb.Message> messages = Arrays.asList(msgs);
            while (!messages.isEmpty()) {
                List<Raftpb.Message> newMsgs = new ArrayList<>();

            }
        }

        public void drop(long from, long to, double perc) {
            this.dropm.put(Connem.builder().from(from).to(to).build(), perc);
        }

        public void cut(long one, long other) {
            this.drop(one, other, 1.0);
            this.drop(other, one, 1.0);
        }

        public void isolate(long id) {
            this.peers.keySet().forEach(i -> {
                long nid = i + 1;
                if (nid != id) {
                    this.drop(id, nid, 1.0);
                    this.drop(nid, id, 1.0);
                }
            });
        }

        public void ignore(Raftpb.MessageType t) {
            this.ignorem.put(t, true);
        }

        public void recover() {
            this.dropm = new HashMap<>();
            this.ignorem = new HashMap<>();
        }

        public List<Raftpb.Message> filter(Raftpb.Message... msgs) {
            List<Raftpb.Message> mm = new ArrayList<>();
            for (Raftpb.Message m : msgs) {
                if (this.ignorem.containsKey(m.getType())) {
                    continue;
                }
                switch (m.getType()) {
                    case MsgHup: {
                        Util.panic("unexpected msgHup");
                        break;
                    }
                    default: {
                        Double perc = this.dropm.get(Connem.builder().from(m.getFrom()).to(m.getTo()).build());
                        float n = ThreadLocalRandom.current().nextFloat();
                        if (n < perc) {
                            continue;
                        }
                    }
                    if (this.msgHook != null) {
                        if (!this.msgHook.apply(m)) {
                            continue;
                        }
                    }
                }
                mm.add(m);
            }
            return mm;
        }
    }

    @Data
    @Builder
    public static class Connem {
        long from, to;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Connem)) return false;

            Connem connem = (Connem) o;

            if (getFrom() != connem.getFrom()) return false;
            return getTo() == connem.getTo();
        }

        @Override
        public int hashCode() {
            int result = (int) (getFrom() ^ (getFrom() >>> 32));
            result = 31 * result + (int) (getTo() ^ (getTo() >>> 32));
            return result;
        }
    }

    class StateMachineImpl implements StateMachine {
        @Override
        public void step(Raftpb.Message m) {

        }

        @Override
        public List<Raftpb.Entry> readMessage() {

            return null;
        }
    }

    public static void setRandomizedElectionTimeout(Raft r, int v) {
        r.setRandomizedElectionTimeout(v);
    }

    public static Raft newTestRaft(long id, List<Long> peers, int election, int heartbeat, Storage storage) throws Exception {
        Config config = newTestConfig(id, peers, election, heartbeat, storage);
        return Raft.newRaft(config);
    }

    public static Raft newTestLearnerRaft(long id, List<Long> peers, List<Long> learner, int election, int heartbeat, Storage storage) throws Exception {
        Config config = newTestConfig(id, peers, election, heartbeat, storage);
        config.setLeaders(learner);
        return Raft.newRaft(config);
    }
}
