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
import java.util.List;

/***
 * cjw
 */
public class RaftFlowControlTest {
    @Test
    public void testMsgAppFlowControlFull() throws Exception {
        Raft r = RaftTest.newTestRaft(1, Arrays.asList(1L,2L), 5, 1, MemoryStorage.newMemoryStorage());
        r.becomeCandidate();
        r.becomeLeader();

        Process pr2 = r.getPrs().get(2L);
        pr2.becomeReplicate();
        for (int i = 0; i < r.getMaxInflight(); i++) {
            r.step(Raftpb.Message.builder()
                    .from(1)
                    .to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            List<Raftpb.Message> ms = RaftTest.readMessages(r);
            if (ms.size() != 1) {
                Assert.fail(String.format("#%d: len(ms) = %d, want 1", i, ms.size()));
            }
        }
        if (!pr2.getIns().full()){
            Assert.fail(String.format("inflights.full = %b, want %b", pr2.ins.full(), true));
        }
        for (int i = 0; i < 10; i++) {
            r.step(Raftpb.Message.builder()
                    .from(1)
                    .to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            List<Raftpb.Message> ms = RaftTest.readMessages(r);
            if (ms.size()!= 0){
                Assert.fail(String.format("#%d: len(ms) = %d, want 0", i, ms.size()));

            }
        }
    }

    @Test
    public void testMsgAppFlowControlMoveForward() throws Exception {
        Raft raft = RaftTest.newTestRaft(1, Arrays.asList(1L, 2L), 5, 1, MemoryStorage.newMemoryStorage());
        raft.becomeCandidate();
        raft.becomeLeader();

        Process pr2 = raft.prs.get(2L);
        pr2.becomeReplicate();

        for (int i = 0; i < raft.maxInflight; i++) {
            raft.step(Raftpb.Message.builder().from(1).to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            RaftTest.readMessages(raft);
        }

        for (int i = 2; i < raft.maxInflight; i++) {
            raft.step(
                    Raftpb.Message.builder()
                            .from(2)
                            .to(1)
                            .type(Raftpb.MessageType.MsgAppResp)
                            .index(i)
                            .build()
            );
            RaftTest.readMessages(raft);
            raft.step(Raftpb.Message.builder()
                    .from(1)
                    .to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());

            List<Raftpb.Message> ms = RaftTest.readMessages(raft);
            if (Util.len(ms)!=1){
                Assert.fail();
            }
            if (!pr2.getIns().full()){
                Assert.fail();
            }
            for (int j = 0; j < i; j++) {
                Raftpb.Message build = Raftpb.Message.builder()
                        .from(2)
                        .to(1)
                        .type(Raftpb.MessageType.MsgAppResp)
                        .index(j)
                        .build();
                raft.step(build);
                if (!pr2.getIns().full()){
                    Assert.fail();
                }
            }
        }
    }
}
