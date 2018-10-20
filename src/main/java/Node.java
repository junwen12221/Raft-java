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

/***
 * cjw
 */
public class Node {
    enum SnapshotStatus {
        SnapshotFinish, SnapshotFailure
    }

    @Builder
    public static class Ready {
        SoftState softState;
        Raftpb.HardState hardState;
        ReadOnly.ReadState[] readStates;
        Raftpb.Entry[] entries;
        Raftpb.Snapshot snapshot;
        Raftpb.Entry[] committedEntries;
        Raftpb.Message[] messages;
        boolean mustSync;
    }

    @Data
    @AllArgsConstructor
    @Builder
    @EqualsAndHashCode
    public static class SoftState {
        long leader;
        long raftState;
    }
    static final Raftpb.HardState emptyState = Raftpb.HardState.builder().build();
}
