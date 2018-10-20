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

import lombok.*;

import java.util.ArrayList;
import java.util.List;

/***
 * cjw
 */
public class Raftpb {
    public enum EntryType {
        Normal, ConfChange,
    }
    @Builder
    @Data
    @EqualsAndHashCode
    public static class Entry {

        private long term;

        private long index;

        private EntryType type;
        private byte[] data;
    }

    @Builder
    @Data
    @EqualsAndHashCode
    public static class ConfState {
        List<Long> nodes;
        List<Long> leaders;
        byte[] XXX_unrecognized ;
    }

    @Builder
    @Data
    @EqualsAndHashCode
    public static class SnapshotMetadata {
        ConfState confState;
        long index;
        long term;
    }

    @Builder
    @Data
    @EqualsAndHashCode
    public static class Snapshot {
        byte[] data;
        SnapshotMetadata metadata;
    }

    public static void main(String[] args) {


    }

    public enum MessageType {
        MsgHup,
        MsgBeat,
        MsgProp,
        MsgApp,
        MsgAppResp,
        MsgVote,
        MsgVoteResp,
        MsgSnap,
        MsgHeartbeat,
        MsgHeartbeatResp,
        MsgUnreachable,
        MsgSnapStatus,
        MsgCheckQuorum,
        MsgTransferLeader,
        MsgTimeoutNow,
        MsgReadIndex,
        MsgReadIndexResp,
        MsgPreVote,
        MsgPreVoteResp,
    }

    @Builder
    @Data
    public static class Message {
        MessageType type;
        long to, from, term, logTerm, index;
        List<Entry> entries;
        long commit;
        Snapshot snapshot;
        boolean reject;
        long rejectHint;
        byte[] context;
    }

    @Builder
    @Data
    static class HardState {
        @NonNull
        long term;
        @NonNull
        long vote;
        @NonNull
        long commit;
    }

    static enum ConfChangeType {
        ConfChangeAddNode,
        ConfChangeRemoveNode,
        ConfChangeUpdateNode,
        ConfChangeAddLearnerNode
    }
    @Builder
    @Data
    static class ConfChange {
        @NonNull
        long id;
        @NonNull
        ConfChangeType type;
        @NonNull
        long nodeId;
        byte[] context;
    }
}
