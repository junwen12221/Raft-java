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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
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
}
