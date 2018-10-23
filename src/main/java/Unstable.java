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
@Data
@Builder
@EqualsAndHashCode
@ToString
public class Unstable {
    Raftpb.Snapshot snapshot;
    List<Raftpb.Entry> entries;
    long offset;
    RaftLogger logger;

    @Builder
    @Value
    static class MaybeResult {
        long value;
        boolean hasSnapshot;
    }

    final static MaybeResult NULL = MaybeResult.builder()
            .value(0)
            .hasSnapshot(false)
            .build();

    public MaybeResult maybeFirstIndex() {
        if (this.snapshot != null) {
            return MaybeResult.builder()
                    .value(this.snapshot.getMetadata().getIndex() + 1)
                    .hasSnapshot(true)
                    .build();
        }
        return NULL;
    }

    public MaybeResult maybeLastIndex() {
        int l = this.entries.size();
        if (l != 0) {
            return MaybeResult.builder()
                    .value(this.offset + l - 1)
                    .hasSnapshot(true)
                    .build();
        }
        if (this.snapshot != null) {
            return MaybeResult.builder()
                    .value(this.snapshot.getMetadata().getIndex())
                    .hasSnapshot(true)
                    .build();
        }
        return NULL;
    }

    public MaybeResult maybeTerm(long i) {
        if (i < this.offset) {
            if (this.snapshot == null) {
                return NULL;
            }
            if (this.snapshot.getMetadata().getIndex() == i) {
                return MaybeResult.builder()
                        .value(this.snapshot.getMetadata().getTerm())
                        .hasSnapshot(true)
                        .build();
            }
            return NULL;
        }
        MaybeResult maybeResult = this.maybeLastIndex();
        long last = maybeResult.getValue();
        boolean ok = maybeResult.isHasSnapshot();

        if (!ok) {
            return NULL;
        }

        if (i > last) {
            return NULL;
        }
        return MaybeResult.builder()
                .value(this.entries.get(((int) (i - this.offset))).getTerm())
                .hasSnapshot(true)
                .build();
    }

    public void stableTo(long i, long t) {
        MaybeResult maybeResult = this.maybeTerm(i);
        long gt = maybeResult.getValue();
        boolean ok = maybeResult.isHasSnapshot();
        if (!ok) return;
        if (gt == t && i >= this.offset) {
            this.entries = this.entries.subList((int) (i + 1 - this
                    .offset), this.entries.size());
            this.offset = i + 1;
            this.shrinkEntriesArray();
        }
    }

    public void shrinkEntriesArray() {
        final int lenMultipe = 2;
        if (this.entries.size() == 0) {
            //this.entries = null;
        } else if (this.entries.size() * lenMultipe < Integer.MAX_VALUE) {
            //this.entries = new ArrayList<>(this.entries);
        }
    }

    public void stableSnapTo(long i) {
        if (this.snapshot != null && this.snapshot.getMetadata().getIndex() == i) {
            this.snapshot = null;
        }
    }

    public void restore(Raftpb.Snapshot s) {
        this.offset = s.getMetadata().getIndex() + 1;
        this.entries = new ArrayList<>();
        this.snapshot = s;
    }

    public void truncateAndAppend(List<Raftpb.Entry> ents) {
        long after = ents.get(0).getIndex();
        if (after == this.offset + this.entries.size()) {
            this.entries = new ArrayList<>(this.entries);
            this.entries.addAll(ents);
        } else if (after <= this.offset) {
            this.logger.infof("replace the unstable entries from index %d", after);
            this.offset = after;
            this.entries = ents;
        } else {
            logger.infof("truncate the unstable entries before index %d", after);
            this.entries = new ArrayList<>(this.slice(this.offset, after));
            this.entries.addAll(ents);
        }
    }

    public List<Raftpb.Entry> slice(long lo, long hi) {
        this.mustCheckOutOfBounds(lo, hi);
        return this.entries.subList ((int) (lo - this.offset), (int) (hi - this.offset));

    }

    public void mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            Util.panic("invalid unstable.slice %d > %d", lo, hi);
        }
        long upper = this.offset + this.entries.size();
        if (lo < this.offset || hi > upper) {
            this.logger.panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, this.offset, upper);
        }
    }
//    public void stableSnapTo(long i){
//        if (this.snapshot!=null&&this.snapshot.getMetadata().getIndex()==i){
//            this.snapshot = null;
//        }
//    }
}
