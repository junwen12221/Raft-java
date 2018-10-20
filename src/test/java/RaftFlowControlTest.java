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
    /**
     * TestMsgAppFlowControlFull ensures:
     * 1. msgApp can fill the sending window until full
     * /2. when the window is full, no more msgApp can be sent.
     * @finished
     * @throws Exception
     */
    @Test
    public void testMsgAppFlowControlFull() throws Exception {
        Raft r = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L), 5, 1, MemoryStorage.newMemoryStorage());
        r.becomeCandidate();
        r.becomeLeader();

        Process pr2 = r.getPrs().get(2L);
        // force the progress to be in replicate state
        pr2.becomeReplicate();

        // fill in the inflights window
        for (int i = 0; i < r.getMaxInflight(); i++) {
            r.step(Raftpb.Message.builder()
                    .from(1)
                    .to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            List<Raftpb.Message> ms = RaftTestUtil.readMessages(r);
            if (ms.size() != 1) {
                Assert.fail(String.format("#%d: len(ms) = %d, want 1", i, ms.size()));
            }
        }

        // ensure 1
        if (!pr2.getIns().full()) {
            Assert.fail(String.format("inflights.full = %b, want %b", pr2.ins.full(), true));
        }

        // ensure 2
        for (int i = 0; i < 10; i++) {
            r.step(Raftpb.Message.builder()
                    .from(1)
                    .to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            List<Raftpb.Message> ms = RaftTestUtil.readMessages(r);
            if (ms.size() != 0) {
                Assert.fail(String.format("#%d: len(ms) = %d, want 0", i, ms.size()));
            }
        }
    }

    /**
     * TestMsgAppFlowControlMoveForward ensures msgAppResp can move
     * forward the sending window correctly:
     * 1. valid msgAppResp.index moves the windows to pass all smaller or equal index.
     * 2. out-of-dated msgAppResp has no effect on the sliding window.
     * @finished
     * @throws Exception
     */
    @Test
    public void testMsgAppFlowControlMoveForward() throws Exception {
        Raft raft = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L), 5, 1, MemoryStorage.newMemoryStorage());
        raft.becomeCandidate();
        raft.becomeLeader();

        Process pr2 = raft.prs.get(2L);
        // force the progress to be in replicate state
        pr2.becomeReplicate();
        // fill in the inflights window

        for (int i = 0; i < raft.maxInflight; i++) {
            raft.step(Raftpb.Message.builder().from(1).to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            RaftTestUtil.readMessages(raft);
        }
        // 1 is noop, 2 is the first proposal we just sent.
        // so we start with 2.
        for (int i = 2; i < raft.maxInflight; i++) {
            // move forward the window
            raft.step(
                    Raftpb.Message.builder()
                            .from(2)
                            .to(1)
                            .type(Raftpb.MessageType.MsgAppResp)
                            .index(i)
                            .build()
            );
            RaftTestUtil.readMessages(raft);

            // fill in the inflights window again
            raft.step(Raftpb.Message.builder()
                    .from(1)
                    .to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());

            List<Raftpb.Message> ms = RaftTestUtil.readMessages(raft);
            if (Util.len(ms) != 1) {
                Assert.fail();
            }

            // ensure 1
            if (!pr2.getIns().full()) {
                Assert.fail();
            }

            // ensure 2
            for (int j = 0; j < i; j++) {
                Raftpb.Message build = Raftpb.Message.builder()
                        .from(2)
                        .to(1)
                        .type(Raftpb.MessageType.MsgAppResp)
                        .index(j)
                        .build();
                raft.step(build);
                if (!pr2.getIns().full()) {
                    Assert.fail();
                }
            }
        }
    }

    /**
     * TestMsgAppFlowControlRecvHeartbeat ensures a heartbeat response
     * frees one slot if the window is full.
     * @finished
     * @throws Exception
     * @finished
     */
    @Test
    public void testMsgAppFlowControlRecvHeartbeat() throws Exception {
        Raft raft = RaftTestUtil.newTestRaft(1, Arrays.asList(1L, 2L), 5, 1, MemoryStorage.newMemoryStorage());
        raft.becomeCandidate();
        raft.becomeLeader();

        Process pr2 = raft.getPrs().get(2L);
        // force the progress to be in replicate state
        pr2.becomeReplicate();
        // fill in the inflights window

        for (int i = 0; i < raft.maxInflight; i++) {
            raft.step(Raftpb.Message.builder().from(1).to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            RaftTestUtil.readMessages(raft);
        }

        for (int tt = 1; tt < 5; tt++) {
            if (!pr2.getIns().full()) {
                Assert.fail(String.format("#%d: inflights.full = %t, want %t", tt, pr2.ins.full(), true));
            }

            // recv tt msgHeartbeatResp and expect one free slot
            for (int i = 0; i < tt; i++) {
                raft.step(Raftpb.Message.builder().from(2).to(1)
                        .type(Raftpb.MessageType.MsgHeartbeatResp)
                        .build());
                RaftTestUtil.readMessages(raft);
                if (pr2.getIns().full()){
                    Assert.fail(String.format("#%d.%d: inflights.full = %s, want %b", tt, i, pr2.ins.full(), false));
                }
            }
            // one slot
            raft.step(Raftpb.Message.builder().from(1).to(1)
                    .type(Raftpb.MessageType.MsgProp)
                    .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                    .build());
            List<Raftpb.Message> ms = RaftTestUtil.readMessages(raft);
            if (Util.len(ms)!=1){
                Assert.fail(String.format("#%d: free slot = 0, want 1", tt));
            }
            for (int i = 0; i < 10; i++) {
                raft.step(Raftpb.Message.builder()
                        .from(1).to(1).type(Raftpb.MessageType.MsgProp)
                        .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                        .build());
                List<Raftpb.Message> ms1 = RaftTestUtil.readMessages(raft);
                if (Util.len(ms1)!=0){
                    Assert.fail(String.format("#%d.%d: len(ms) = %d, want 0", tt, i, Util.len(ms1)));
                }
            }

            // clear all pending messages.
            raft.step(Raftpb.Message.builder()
                    .from(2).to(1).type(Raftpb.MessageType.MsgHeartbeatResp)
                    .build());
            RaftTestUtil.readMessages(raft);
        }


    }
}
