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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 * cjw
 */
public class RaftSnapTest {
    final static Raftpb.Snapshot testingSnap = Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder()
            .index(11).term(11).confState(Raftpb.ConfState.builder()
                    .nodes(Arrays.asList(1L,2L))
                    .leaders(new ArrayList<>())
                    .build()).build()).build();
    @Test
    public void testSendingSnapshotSetPendingSnapshot()throws Exception{
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        Raft sm = RaftTestUtil.newTestRaft(1, Arrays.asList(1L), 10, 1, storage);
        sm.restore(testingSnap);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.prs.get(2L).setNext(sm.raftLog.firstIndex());

        sm.step(Raftpb.Message.builder().from(2).to(1).type(Raftpb.MessageType.MsgAppResp)
                .index(sm.prs.get(2L).getNext()-1)
                .reject(true)
                .build());

        if (sm.prs.get(2L).pendingSnapshot!=11){
            Assert.fail();
        }
    }
    @Test
    public void testPendingSnapshotPauseReplication()throws Exception{
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        Raft sm = RaftTestUtil.newTestRaft(1, Arrays.asList(1L,2L), 10, 1, storage);
        sm.restore(testingSnap);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.prs.get(2L).becomeSnapshot(11);

        sm.step(Raftpb.Message.builder().from(1).to(1).type(Raftpb.MessageType.MsgProp)
                .entries(Arrays.asList(Raftpb.Entry.builder().data("somedata".getBytes()).build()))
                .build());

        List<Raftpb.Message> msgs = RaftTestUtil.readMessages(sm);
        if ( Util.len(msgs) != 0){
            Assert.fail();
        }
    }

    @Test
    public void testSnapshotFailure()throws Exception{
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        Raft sm = RaftTestUtil.newTestRaft(1, Arrays.asList(1L,2L), 10, 1, storage);
        sm.restore(testingSnap);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.prs.get(2L).setNext(1);
        sm.prs.get(2L).becomeSnapshot(11);

        sm.step(Raftpb.Message.builder().from(2).to(1).type(Raftpb.MessageType.MsgSnapStatus)
                .reject(true)
                .build());
        Process prs = sm.prs.get(2L);

        if (prs.pendingSnapshot != 0){
            Assert.fail();
        }
        if (prs.next != 1){
            Assert.fail();
        }
        if (!prs.paused ){
            Assert.fail();
        }
    }

    @Test
    public void testSnapshotSucceed()throws Exception {
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        Raft sm = RaftTestUtil.newTestRaft(1, Arrays.asList(1L,2L), 10, 1, storage);
        sm.restore(testingSnap);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.prs.get(2L).setNext(1);
        sm.prs.get(2L).becomeSnapshot(11);

        sm.step(Raftpb.Message.builder().from(2).to(1).type(Raftpb.MessageType.MsgSnapStatus)
                .reject(false)
                .build());
        Process prs = sm.prs.get(2L);

        if (prs.pendingSnapshot != 0) {
            Assert.fail();
        }
        if (prs.next != 12) {
            Assert.fail();
        }
        if (!prs.paused) {
            Assert.fail();
        }
    }
    @Test
    public void testSnapshotAbort()throws Exception {
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        Raft sm = RaftTestUtil.newTestRaft(1,Arrays.asList(1L,2L), 10, 1, storage);
        sm.restore(testingSnap);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.prs.get(2L).setNext(1);
        sm.prs.get(2L).becomeSnapshot(11);

        sm.step(Raftpb.Message.builder().from(2).to(1).type(Raftpb.MessageType.MsgAppResp)
                .index(11)
                .build());
        Process prs = sm.prs.get(2L);

        if (prs.pendingSnapshot != 0) {
            Assert.fail();
        }
        if (prs.next != 12) {
            Assert.fail();
        }
    }
}
