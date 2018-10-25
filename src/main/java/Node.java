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
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/***
 * cjw
 */
public interface Node {

    Raftpb.HardState emptyState = Raftpb.HardState.builder().build();


    enum SnapshotStatus {
        SnapshotFinish, SnapshotFailure
    }

    Exception ErrStopped = new Exception("raft: stopped");

    static boolean isHardStateEqual(Raftpb.HardState a, Raftpb.HardState b) {
        return a.term == b.term && a.vote == b.vote && a.commit == b.commit;
    }

    static boolean isEmptyHardState(Raftpb.HardState b) {
        return isHardStateEqual(b, emptyState);
    }

    static boolean isEmptySnap(Raftpb.Snapshot sp) {
        return sp.metadata.index == 0;
    }

    void tick();

    @Builder
    class Ready {
        SoftState softState;
        Raftpb.HardState hardState;
        List<ReadOnly.ReadState> readStates;
        List<Raftpb.Entry> entries;
        Raftpb.Snapshot snapshot;
        List<Raftpb.Entry> committedEntries;
        List<Raftpb.Message> messages;
        boolean mustSync;

        public boolean containsUpdate() {
            return this.softState != null || !isEmptyHardState(this.hardState) ||
                    !isEmptySnap(this.snapshot) || Util.len(this.entries) > 0 ||
                    Util.len(this.committedEntries) > 0 || Util.len(this.messages) > 0 || Util.len(this.readStates) != 0;
        }
    }

    @Data
    @AllArgsConstructor
    @Builder
    @EqualsAndHashCode
    class SoftState {
        long leader;
        long raftState;
    }


}
