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

import java.io.Closeable;
import java.util.List;

public interface Storage extends Closeable {
    // ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
    final static Exception ErrCompacted = new Exception("requested index is unavailable due to compaction");

    // ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
    final static Exception ErrSnapOutOfDate = new Exception("requested index is older than the existing snapshot");

    // ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
    final static Exception ErrUnavailable = new Exception("requested entry at index is unavailable");
    final static Exception ErrSnapshotTemporarilyUnavailable = new Exception("snapshot is temporarily unavailable");

    @Builder
    @Data
    public static class State {
        Raftpb.HardState hardState;
        Raftpb.ConfState confState;
    }

    State initialSate() throws Exception;

    List<Raftpb.Entry> entries(long lo, long hi, long maxsize) throws Exception;

    long term(long i) throws Exception;

    long lastIndex() throws Exception;

    long firstIndex() throws Exception;

    Raftpb.Snapshot snapshot() throws Exception;
}
