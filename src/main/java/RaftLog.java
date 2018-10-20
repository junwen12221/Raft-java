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

import java.util.ArrayList;
import java.util.List;

/***
 * cjw
 */
@Data
public class RaftLog {
    Storage storage;
    RaftLogger logger;
    Unstable unstable;
    long applied, maxMsgSize, committed;

    public RaftLog(Storage storage, RaftLogger logger, long maxMsgSize) {
        this.storage = storage;
        this.logger = logger;
        this.maxMsgSize = maxMsgSize;
    }

    public static RaftLog newLogWithSize(Storage storage, RaftLogger logger, long maxMsgSize) throws Exception {
        if (storage == null) {
            Util.panic("storage must not be nil");
        }
        RaftLog log = new RaftLog(storage, logger, maxMsgSize);
        long firstIndex = storage.firstIndex();
        long lastIndex = storage.lastIndex();
        log.unstable = Unstable.builder().entries(new ArrayList<>()).build();
        log.unstable.offset = lastIndex + 1;
        log.unstable.logger = logger;
        log.committed = firstIndex - 1;
        log.applied = firstIndex - 1;
        return log;
    }
    public static RaftLog newLog(Storage storage, RaftLogger logger) throws Exception {
        return newLogWithSize(storage, logger, Raft.noLimit);
    }

    public static RaftLog newLog(Storage storage, RaftLogger logger, long maxMsgSize) throws Exception {
        return newLogWithSize(storage, logger, maxMsgSize);
    }

    @Data
    @Builder
    public static class MaybeAppendResult {
        long lastnewi;
        boolean ok;
    }

    public static final MaybeAppendResult NULL = MaybeAppendResult.builder().lastnewi(0L).ok(false).build();

    public MaybeAppendResult maybeAppend(long index, long logTerm, long commited, List<Raftpb.Entry> ents) throws Exception {
        if (this.matchTerm(index, logTerm)) {
            if (ents == null){
                ents = new ArrayList<>();
            }
            long lastnewi = index + ents.size();
            long ci = this.finConflct(ents);
            if (ci == 0L) {

            } else if (ci <= this.committed) {
                this.logger.panicf("entry %d conflict with committed entry [committed(%d)]", ci, this.committed);
            } else {
                long offset = index + 1;
                this.append(ents.subList((int) (ci - offset), ents.size()));
            }
            this.commitTo(Math.min(commited, lastnewi));
            return MaybeAppendResult.builder().lastnewi(lastnewi).ok(true).build();
        }
        return NULL;
    }


    public long append(List<Raftpb.Entry> ents) throws Exception {
        if (ents == null){
            ents = new ArrayList<>();
        }
        if (ents.size() == 0) {
            return this.lastIndex();
        }
        long after = ents.get(0).getIndex() - 1;
        if (after < this.getCommitted()) {
            logger.panicf("after(%d) is out of range [committed(%d)]", after, this.committed);
        }
        this.unstable.truncateAndAppend(ents);
        return this.lastIndex();
    }


    public long finConflct(List<Raftpb.Entry> ents) throws Exception {

        for (Raftpb.Entry ne : ents) {
            if (!this.matchTerm(ne.getIndex(), ne.getTerm())) {
                if (ne.getIndex() <= this.lastIndex()) {

                }
                return ne.getIndex();
            }
        }
        return 0;
    }

    public List<Raftpb.Entry> unstableEntries() {
        if (this.unstable.entries.isEmpty()) return null;
        return this.unstable.entries;
    }

    public List<Raftpb.Entry> nextEnts() throws Exception {
        long off = Math.max(this.applied + 1, this.firstIndex());
        if (this.committed + 1 > off) {
            return this.slice(off, this.committed + 1, this.maxMsgSize);
        }
        return null;
    }

    public boolean hasNextEnts() throws Exception {
        long off = Math.max(this.applied + 1, this.firstIndex());
        return (this.committed + 1) > off;
    }

    public Raftpb.Snapshot snapshot() throws Exception {
        Raftpb.Snapshot snapshot = this.unstable.getSnapshot();
        if (snapshot != null) {
            return snapshot;
        }
        return this.storage.snapshot();
    }

    public long firstIndex() throws Exception {
        Unstable.MaybeResult maybeResult = this.unstable.maybeFirstIndex();
        if (maybeResult.isHasSnapshot()) {
            return maybeResult.getValue();
        }
        return this.storage.firstIndex();
    }

    public long lastIndex() throws Exception {
        Unstable.MaybeResult maybeResult = this.unstable.maybeLastIndex();
        if (maybeResult.isHasSnapshot()) {
            return maybeResult.getValue();
        }
        return this.storage.lastIndex();
    }

    public long lastTerm() throws Exception {
        try {
            return this.term(this.lastIndex());
        } catch (Exception err) {
            Util.panic("unexpected errorf when getting the last term (%v)", err);
        }
        return 0L;
    }

    public long term(long i) throws Exception {
        long dummyIndex = this.firstIndex() - 1;
        if (i < dummyIndex || i > this.lastIndex()) {
            return 0L;
        }
        Unstable.MaybeResult maybeResult = this.unstable.maybeTerm(i);
        if (maybeResult.isHasSnapshot()) {
            return maybeResult.getValue();
        }
        return this.storage.term(i);
    }

    public void commitTo(long tocommit)throws Exception {
        if (this.committed<tocommit){
            if (this.lastIndex()<tocommit){
                this.logger.panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, this.lastIndex());
            }
            this.committed = tocommit;
        }
        return;
    }

    public void appliesTo(long i) {
        if (i == 0L) {
            return;
        }
        if (this.committed < i || i < this.applied) {

        }
        this.applied = i;
    }

    public List<Raftpb.Entry> entries(long i, long maxsize) throws Exception {
        if (i > this.lastIndex()) {
            return null;
        }
        long l = this.lastIndex() + 1;
        return this.slice(i, l, maxsize);
    }

    public List<Raftpb.Entry> allEntries() throws Exception {
        try {
            List<Raftpb.Entry> ents = this.entries(this.firstIndex(), Raft.noLimit);
            return ents;
        } catch (Exception e) {
            if (e == Storage.ErrCompacted) {
                return this.allEntries();
            }
            Util.panic(e);
        }
        return null;
    }

    public boolean isUpToDate(long lasti, long term) throws Exception {
        return term > this.lastTerm() || (term == this.lastTerm() && lasti >= this.lastIndex());
    }

    public boolean matchTerm(long i, long term) {
        try {
            long t = this.term(i);
            return t == term;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean matchCommit(long maxIndex, long term) throws Exception {
        if (maxIndex > this.committed) {
            Exception err = null;
            long t = 0L;
            try {
                t = this.term(maxIndex);
            } catch (Exception e) {
                err = e;
            }
            if (this.zeroTermOnErrCompacted(t, err) == term) {
                this.commitTo(maxIndex);
                return true;
            }
        }
        return false;
    }

    public void restore(Raftpb.Snapshot s) {
        this.committed = s.getMetadata().getIndex();
        this.unstable.restore(s);
    }

    public List<Raftpb.Entry> slice(long lo, long hi, long maxSize) throws Exception {
        Exception exception = this.mustCheckOutOfBounds(lo, hi);
        if (exception != null) {
            return null;
        }
        if (lo == hi) {
            return null;
        }
        List<Raftpb.Entry> ents = null;
        if (lo < this.unstable.offset) {
            try {
                List<Raftpb.Entry> storeEnts = this.storage.entries(lo, Math.min(hi, this.unstable.offset), maxSize);
                if (storeEnts.size() < Math.min(hi, this.unstable.offset) - lo) {
                    return storeEnts;
                }
                ents = storeEnts;
            } catch (Exception e) {
                if (e == Storage.ErrUnavailable) {
                    Util.panic("entries[%d:%d) is unavailable from storage", lo, Math.min(hi, this.unstable.offset));
                }
                throw e;
            }
        }
        if (hi > this.unstable.offset) {
            List<Raftpb.Entry> unstable = this.unstable.slice(Math.max(lo, this.unstable.offset), hi);
            if (ents == null || ents.isEmpty()) {
                ents = unstable;
            } else {
                ents.addAll(unstable);
            }
        }
        return Util.limitSize(ents, maxSize);
    }

    public Exception mustCheckOutOfBounds(long lo, long hi) throws Exception {
        if (lo > hi) {

        }
        long fi = this.firstIndex();
        if (lo < fi) {
            throw Storage.ErrCompacted;
        }
        long length = this.lastIndex() + 1 + -fi;
        if (lo < fi || hi > fi + length) {

        }
        return null;
    }

    public long zeroTermOnErrCompacted(long t, Exception error) {
        if (error == null) {
            return t;
        }
        if (error == Storage.ErrCompacted) {
            return 0L;
        }
        Util.panic("unexpected errorf (%v)", error);
        return 0;
    }

    public void stableTo(long i,long t){
        this.unstable.stableTo(i,t);
    }
    public void stableSnapTo(long i){
        this.unstable.stableSnapTo(i);
    }
}