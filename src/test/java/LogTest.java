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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LogTest {
    @Data
    @Builder
    @AllArgsConstructor
    static class FindConflictCase {
        List<Raftpb.Entry> ents;
        long wconfict;
    }

    FindConflictCase FindConflictCase(List<Raftpb.Entry> ents, long wconfict) {
        return new FindConflictCase(ents, wconfict);
    }

    @Test
    public void testFindConflict() throws Exception {
        List<Raftpb.Entry> previousEnts = Arrays.asList(StorageTest.entry(1, 1), StorageTest.entry(2, 2), StorageTest.entry(3, 3));

        List<FindConflictCase> tests = Arrays.asList(FindConflictCase(Arrays.asList(), 0),
                FindConflictCase(Arrays.asList(StorageTest.entry(1, 1), StorageTest.entry(2, 2), StorageTest.entry(3, 3)), 0),
                FindConflictCase(Arrays.asList(StorageTest.entry(2, 2), StorageTest.entry(3, 3)), 0),
                FindConflictCase(Arrays.asList(StorageTest.entry(3, 3)), 0),
                FindConflictCase(Arrays.asList(StorageTest.entry(1, 1), StorageTest.entry(2, 2), StorageTest.entry(3, 3)
                        , StorageTest.entry(4, 4), StorageTest.entry(5, 4)), 4),

                FindConflictCase(Arrays.asList(StorageTest.entry(4, 4), StorageTest.entry(5, 4)), 4),
                FindConflictCase(Arrays.asList(StorageTest.entry(1, 4), StorageTest.entry(2, 4)), 1),
                FindConflictCase(Arrays.asList(StorageTest.entry(2, 1), StorageTest.entry(3, 4), StorageTest.entry(4, 4)), 2),
                FindConflictCase(Arrays.asList(StorageTest.entry(3, 1), StorageTest.entry(4, 2), StorageTest.entry(5, 4), StorageTest.entry(6, 4)), 3)
        );

        int size = tests.size();
        for (int i = 0; i < size; i++) {
            FindConflictCase tt = tests.get(i);
            RaftLog raftLog = RaftLog.newLog(MemoryStorage.newMemoryStorage(), RaftLogger.raftLogger);
            raftLog.append(previousEnts);
            long gconfict = raftLog.finConflct(tt.getEnts());

            if (gconfict != tt.wconfict) {
                Assert.fail();
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class IsUpToDateCase {
        long lastIndex;
        long term;
        boolean wUpToDate;
    }

    IsUpToDateCase IsUpToDate(long lastIndex,
                              long term,
                              boolean wUpToDate) {
        return new IsUpToDateCase(lastIndex, term, wUpToDate);
    }

    @Test
    public void testIsUpToDate() throws Exception {
        List<Raftpb.Entry> previousEnts = Arrays.asList(StorageTest.entry(1, 1), StorageTest.entry(2, 2), StorageTest.entry(3, 3));
        RaftLog raftLog = RaftLog.newLog(MemoryStorage.newMemoryStorage(), RaftLogger.raftLogger);
        raftLog.append(previousEnts);

        List<IsUpToDateCase> tests = Arrays.asList(
                IsUpToDate(raftLog.lastIndex() - 1, 4, true),
                IsUpToDate(raftLog.lastIndex(), 4, true),
                IsUpToDate(raftLog.lastIndex() + 1, 4, true),

                IsUpToDate(raftLog.lastIndex() - 1, 2, false),
                IsUpToDate(raftLog.lastIndex(), 2, false),
                IsUpToDate(raftLog.lastIndex() + 1, 2, false),

                IsUpToDate(raftLog.lastIndex() - 1, 3, false),
                IsUpToDate(raftLog.lastIndex(), 3, true),
                IsUpToDate(raftLog.lastIndex() + 1, 3, true)
        );
        int size = tests.size();
        for (int i = 0; i < size; i++) {
            IsUpToDateCase tt = tests.get(i);
            boolean upToDate = raftLog.isUpToDate(tt.lastIndex, tt.term);
            if (upToDate != tt.wUpToDate) {
                Assert.fail();
            }
        }
    }

    @Data
    @AllArgsConstructor
    @Builder
    static class AppendCase {
        List<Raftpb.Entry> ents;
        long windex;
        List<Raftpb.Entry> wents;
        long wunstable;
    }

    AppendCase AppendCase(
            List<Raftpb.Entry> ents,
            long windex,
            List<Raftpb.Entry> wents,
            long wunstable
    ) {
        return new AppendCase(ents, windex, wents, wunstable);
    }

    @Test
    public void testAppend() throws Exception {
        List<Raftpb.Entry> previousEnts = StorageTest.entries(StorageTest.entry(1, 1), StorageTest.entry(2, 2));
        AppendCase a1 = AppendCase(Arrays.asList(),
                2,
                Arrays.asList(StorageTest.entry(1, 1), StorageTest.entry(2, 2)),
                3);
        AppendCase a2 = AppendCase(Arrays.asList(StorageTest.entry(3, 2)),
                3,
                Arrays.asList(StorageTest.entry(1, 1), StorageTest.entry(2, 2), StorageTest.entry(3, 2)),
                3);
        AppendCase a3 = AppendCase(Arrays.asList(StorageTest.entry(1, 2)),
                1,
                Arrays.asList(StorageTest.entry(1, 2)),
                1);
        AppendCase a4 = AppendCase(Arrays.asList(StorageTest.entry(2, 3), StorageTest.entry(3, 3)),
                3,
                Arrays.asList(StorageTest.entry(1, 1), StorageTest.entry(2, 3), StorageTest.entry(3, 3)),
                2);
        List<AppendCase> tests = Arrays.asList(a1, a2, a3, a4);
        int size = tests.size();
        for (int i = 0; i < size; i++) {
            AppendCase tt = tests.get(i);
            MemoryStorage s = MemoryStorage.newMemoryStorage();
            s.append(previousEnts);
            RaftLog raftLog = RaftLog.newLog(s, RaftLogger.raftLogger);
            long index = raftLog.append(tt.ents);
            if (index != tt.windex) {
                Assert.fail();
            }

            List<Raftpb.Entry> g = raftLog.entries(1, Raft.noLimit);
            if (!g.equals(tt.wents)) {
                Assert.fail();
            }
            long goff = raftLog.unstable.offset;
            if (goff != tt.wunstable) {
                Assert.fail();
            }

        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class LogMaybeAppendCase {
        long logTerm;
        long index;
        long commited;
        List<Raftpb.Entry> ents;

        long wlasti;
        boolean wappend;
        long wcommit;
        boolean wpanic;
    }

    public LogMaybeAppendCase LogMaybeAppendCase(long logTerm,
                                                 long index,
                                                 long commited,
                                                 List<Raftpb.Entry> ents,

                                                 long wlasti,
                                                 boolean wappend,
                                                 long wcommit,
                                                 boolean wpanic) {
        return new LogMaybeAppendCase(logTerm, index, commited, ents, wlasti, wappend, wcommit, wpanic);
    }

    Raftpb.Entry entry(long index, long term) {
        return StorageTest.entry(index, term);
    }

    List<Raftpb.Entry> entries(Raftpb.Entry... entries) {
        return StorageTest.entries(entries);
    }

    @Test
    public void testLogMaybeAppend() throws Exception {
        List<Raftpb.Entry> previousEnts = Arrays.asList(entry(1, 1), entry(2, 2), entry(3, 3));
        long lastIndex = 3L;
        long lastTerm = 3L;
        long commit = 1L;
        LogMaybeAppendCase l1 = LogMaybeAppendCase(lastTerm - 1, lastIndex, lastIndex, entries(entry(lastIndex + 1, 4)),
                0, false, commit, false);
        LogMaybeAppendCase l2 = LogMaybeAppendCase(lastTerm, lastIndex + 1, lastIndex, entries(entry(lastIndex + 2, 4)),
                0, false, commit, false);
        LogMaybeAppendCase l3 = LogMaybeAppendCase(lastTerm, lastIndex, lastIndex, null,
                lastIndex, true, lastIndex, false);
        LogMaybeAppendCase l4 = LogMaybeAppendCase(lastTerm, lastIndex, lastIndex + 1, null,
                lastIndex, true, lastIndex, false);
        LogMaybeAppendCase l5 = LogMaybeAppendCase(lastTerm, lastIndex, lastIndex - 1, null,
                lastIndex, true, lastIndex - 1, false);
        LogMaybeAppendCase l6 = LogMaybeAppendCase(lastTerm, lastIndex, 0, null,
                lastIndex, true, commit, false);
        LogMaybeAppendCase l7 = LogMaybeAppendCase(0, 0, lastIndex, null,
                0, true, commit, false);
        LogMaybeAppendCase l8 = LogMaybeAppendCase(lastTerm, lastIndex, lastIndex, entries(entry(lastIndex + 1, 4)),
                lastIndex + 1, true, lastIndex, false);
        LogMaybeAppendCase l9 = LogMaybeAppendCase(lastTerm, lastIndex, lastIndex + 1, entries(entry(lastIndex + 1, 4)),
                lastIndex + 1, true, lastIndex + 1, false);
        LogMaybeAppendCase l10 = LogMaybeAppendCase(lastTerm, lastIndex, lastIndex + 2, entries(entry(lastIndex + 1, 4)),
                lastIndex + 1, true, lastIndex + 1, false);
        LogMaybeAppendCase l11 = LogMaybeAppendCase(lastTerm, lastIndex, lastIndex + 2, entries(entry(lastIndex + 1, 4),
                entry(lastIndex + 2, 4)),
                lastIndex + 2, true, lastIndex + 2, false);
        LogMaybeAppendCase l12 = LogMaybeAppendCase(lastTerm - 1, lastIndex - 1, lastIndex, entries(entry(lastIndex, 4)),
                lastIndex, true, lastIndex, false);
        LogMaybeAppendCase l13 = LogMaybeAppendCase(lastTerm - 2, lastIndex - 2, lastIndex, entries(entry(lastIndex - 1, 4)),
                lastIndex - 1, true, lastIndex - 1, false);
        LogMaybeAppendCase l14 = LogMaybeAppendCase(lastTerm - 3, lastIndex - 3, lastIndex, entries(entry(lastIndex - 2, 4)),
                lastIndex - 2, true, lastIndex - 2, true);//@todo check
        LogMaybeAppendCase l15 = LogMaybeAppendCase(lastTerm - 2, lastIndex - 2, lastIndex, entries(entry(lastIndex - 1, 4),
                entry(lastIndex, 4)),
                lastIndex, true, lastIndex, false);
        List<LogMaybeAppendCase> tests = Arrays.asList(
                l1, l2, l3, l4, l5, l6, l7,
                l8, l9, l10, l11, l12, l13,
                l14
                , l15
        );
        int size = tests.size();
        for (int i = 0; i < size; i++) {
            LogMaybeAppendCase tt = tests.get(i);
            RaftLog raftLog = RaftLog.newLog(MemoryStorage.newMemoryStorage(), RaftLogger.raftLogger);
            raftLog.append(previousEnts);
            raftLog.setCommitted(commit);
            try {
                RaftLog.MaybeAppendResult maybeAppendResult = raftLog.maybeAppend(tt.index, tt.logTerm, tt.commited, tt.ents);
                boolean gappend = maybeAppendResult.ok;
                long glasti = maybeAppendResult.lastnewi;
                long gcommit = raftLog.getCommitted();
                if (glasti != tt.wlasti) {
                    Assert.fail();
                }
                if (gappend != tt.wappend) {
                    Assert.fail();
                }
                if (gcommit != tt.wcommit) {
                    Assert.fail();
                }
                if (gappend && Util.len(tt.ents) != 0) {
                    List<Raftpb.Entry> gents = raftLog.slice(raftLog.lastIndex() - tt.ents.size() + 1, raftLog.lastIndex() + 1, Raft.noLimit);
                    if (!tt.ents.equals(gents)) {
                        Assert.fail();
                    }
                }
            } catch (Exception e) {
                if (!tt.wpanic) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testCompactionSideEffects() throws Exception {
        long i = 0;
        long lastIndex = 1000;
        long unstableIndex = 750;
        long lastTerm = lastIndex;
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        for (i = 1; i <= unstableIndex; i++) {
            storage.append(entries(entry(i, i)));
        }
        RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
        for (i = unstableIndex; i < lastIndex; i++) {
            raftLog.append(entries(entry(i + 1, i + 1)));
        }
        boolean ok = raftLog.matchCommit(lastIndex, lastTerm);
        if (ok) {
            Assert.fail();
        }
        raftLog.appliesTo(raftLog.committed);
        long offset = 500;
        storage.compact(offset);
        if (raftLog.lastIndex() != lastIndex) {
            Assert.fail();
        }
        for (long j = offset; j <= raftLog.lastIndex(); j++) {
            Exception err = null;
            long term = 0L;
            try {
                term = raftLog.term(j);
            } catch (Exception e) {
                err = e;
            }
            if (mustTerm(term, err) != j) {
                Assert.fail();
            }
        }
        for (long j = offset; j <= raftLog.lastIndex(); j++) {
            boolean b = raftLog.matchTerm(j, j);
            if (!b) {
                Assert.fail();
            }
        }
        List<Raftpb.Entry> unstableEnts = raftLog.unstableEntries();
        if (unstableEnts.size() != 250) {
            Assert.fail();
        }
        if (unstableEnts.get(0).getIndex() != 751) {
            Assert.fail();
        }
        long prve = raftLog.lastIndex();
        raftLog.append(entries(entry(raftLog.lastIndex() + 1, raftLog.lastIndex() + 1)));
        if (raftLog.lastIndex() != prve + 1) {
            Assert.fail();
        }
        List<Raftpb.Entry> ents = raftLog.entries(raftLog.lastIndex(), Raft.noLimit);
        if (ents.size() != 1) {
            Assert.fail();
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class HasNextEntsCase {
        long applied;
        boolean hasNext;
    }

    HasNextEntsCase HasNextEntsCase(long applied, boolean hasNext) {
        return HasNextEntsCase.builder().applied(applied).hasNext(hasNext).build();
    }

    @Test
    public void testHasNextEnts() throws Exception {
        Raftpb.Snapshot snap = Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().term(1).index(3).build()).build();
        List<Raftpb.Entry> ents = Arrays.asList(
                Raftpb.Entry.builder().term(1).index(4).build(),
                Raftpb.Entry.builder().term(1).index(5).build(),
                Raftpb.Entry.builder().term(1).index(6).build()
        );
        List<HasNextEntsCase> tests = Arrays.asList(
                HasNextEntsCase(0, true),
                HasNextEntsCase(3, true),
                HasNextEntsCase(4, true),
                HasNextEntsCase(5, false)
        );
        for (HasNextEntsCase tt : tests) {
            MemoryStorage storage = MemoryStorage.newMemoryStorage();
            storage.applySnapshot(snap);
            RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
            raftLog.append(ents);
            raftLog.matchCommit(5, 1);
            raftLog.appliesTo(tt.applied);

            boolean hasNextEnts = raftLog.hasNextEnts();
            if (hasNextEnts != tt.hasNext) {
                Assert.fail();
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class NextEntsCase {
        long applied;
        List<Raftpb.Entry> wents;
    }

    NextEntsCase NextEntsCase(long applied,
                              List<Raftpb.Entry> wents) {
        return new NextEntsCase(applied, wents);
    }

    @Test
    public void testNextEnts() throws Exception {
        Raftpb.Snapshot snap = Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder()
                .term(1)
                .index(3)
                .build()).build();
        List<Raftpb.Entry> ents = Arrays.asList(
                entry(4, 1),
                entry(5, 1),
                entry(6, 1)
        );
        List<NextEntsCase> tests = Arrays.asList(
                NextEntsCase(0, ents.subList(0, 2)),
                NextEntsCase(3, ents.subList(0, 2)),
                NextEntsCase(4, ents.subList(1, 2)),
                NextEntsCase(5, null)
        );
        for (NextEntsCase tt : tests) {
            MemoryStorage storage = MemoryStorage.newMemoryStorage();
            storage.applySnapshot(snap);
            RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
            raftLog.append(ents);
            raftLog.matchCommit(5, 1);
            raftLog.appliesTo(tt.applied);

            try {
                List<Raftpb.Entry> nentx = raftLog.nextEnts();
                if (!((nentx == tt.wents) || nentx.equals(tt.wents))) {
                    Assert.fail();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    @AllArgsConstructor
    @Data
    @Builder
    static class UnstableEntsCase {
        int unstable;
        List<Raftpb.Entry> wents;
    }

    UnstableEntsCase UnstableEntsCase(
            int unstable,
            List<Raftpb.Entry> wents
    ) {
        return new UnstableEntsCase(unstable, wents);
    }

    @Test
    public void testUnstableEnts() throws Exception {
        List<Raftpb.Entry> previousEnts = Arrays.asList(entry(1, 1), entry(2, 2));
        List<UnstableEntsCase> tests = Arrays.asList(UnstableEntsCase(3, null), UnstableEntsCase(1, previousEnts));

        int size = tests.size();

        for (int i = 0; i < size; i++) {
            UnstableEntsCase tt = tests.get(i);
            MemoryStorage storage = MemoryStorage.newMemoryStorage();
            storage.append(previousEnts.subList(0, tt.unstable - 1));

            RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
            raftLog.append(previousEnts.subList(tt.unstable - 1, previousEnts.size()));

            List<Raftpb.Entry> ents = raftLog.unstableEntries();
            if (ents != null && ents.size() > 1) {
                raftLog.stableTo(ents.get(ents.size() - 1).getIndex(), ents.get(ents.size() - i).getTerm());
            }
            if (!(ents == tt.wents || ents.equals(tt.wents))) {
                Assert.fail();
            }
            long w = previousEnts.get(previousEnts.size() - 1).getIndex() + 1;
            if (raftLog.unstable.offset != w) {
                Assert.fail();
            }
        }
    }

    @AllArgsConstructor
    @Data
    @Builder
    static class CommitToCase {
        long commit;
        long wcommit;
        boolean wpanic;
    }

    CommitToCase CommitToCase(
            long commit,
            long wcommit,
            boolean wpanic
    ) {
        return new CommitToCase(commit, wcommit, wpanic);
    }

    @Test
    public void testCommitTo() {
        List<Raftpb.Entry> previousEnts = Arrays.asList(entry(1, 1), entry(2, 2), entry(3, 3));
        long commit = 2;

        List<CommitToCase> tests = Arrays.asList(
                CommitToCase(3, 3, false),
                CommitToCase(1, 2, false),
                CommitToCase(4, 0, true)
        );

        for (CommitToCase tt : tests) {
            try {
                MemoryStorage memoryStorage = MemoryStorage.newMemoryStorage();
                RaftLog raftLog = RaftLog.newLog(memoryStorage, RaftLogger.raftLogger);
                raftLog.append(previousEnts);
                raftLog.committed = (commit);
                raftLog.commitTo(tt.commit);
                if (raftLog.committed != tt.wcommit) {
                    Assert.fail();
                }
            } catch (Exception e) {
                if (!tt.wpanic) {
                    Assert.fail();
                }
            }
        }
    }

    @Builder
    @Data
    @AllArgsConstructor
    static class StableToCase {
        long stablei;
        long stablet;
        long wunsatble;
    }

    StableToCase StableToCase(
            long stablei,
            long stablet,
            long wunsatble
    ) {
        return new StableToCase(stablei, stablet, wunsatble);
    }

    @Test
    public void testStableTo() throws Exception {
        List<StableToCase> tests = Arrays.asList(StableToCase(1, 1, 2)
                , StableToCase(2, 2, 3),
                StableToCase(2, 1, 1),
                StableToCase(3, 1, 1));
        for (StableToCase tt : tests) {
            RaftLog raftLog = RaftLog.newLog(MemoryStorage.newMemoryStorage(), RaftLogger.raftLogger);
            raftLog.append(entries(entry(1, 1), entry(2, 2), entry(3, 3)));
            raftLog.stableTo(tt.stablei, tt.stablet);
            if (raftLog.unstable.offset != tt.wunsatble) {
                Assert.fail();
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class StableToWithSnapCase {
        long stablei;
        long stablet;
        List<Raftpb.Entry> newEnts;

        long wunstable;
    }

    StableToWithSnapCase StableToWithSnapCase(
            long stablei,
            long stablet,
            List<Raftpb.Entry> newEnts,

            long wunstable
    ) {
        return new StableToWithSnapCase(stablei, stablet, newEnts, wunstable);
    }

    @Test
    public void testStableToWithSnap() throws Exception {
        long snapi = 5;
        long snapt = 2;

        List<StableToWithSnapCase> tests = Arrays.asList(
                StableToWithSnapCase(snapi + 1, snapt, null, snapi + 1),
                StableToWithSnapCase(snapi, snapt, null, snapi + 1),
                StableToWithSnapCase(snapi - 1, snapt, null, snapi + 1),

                StableToWithSnapCase(snapi + 1, snapt + 1, null, snapi + 1),
                StableToWithSnapCase(snapi, snapt + 1, null, snapi + 1),
                StableToWithSnapCase(snapi - 1, snapt + 1, null, snapi + 1),

                StableToWithSnapCase(snapi + 1, snapt, entries(entry(snapi + 1, snapt)), snapi + 2),
                StableToWithSnapCase(snapi, snapt + 1, entries(entry(snapi + 1, snapt)), snapi + 1),
                StableToWithSnapCase(snapi - 1, snapt + 1, entries(entry(snapi + 1, snapt)), snapi + 1),

                StableToWithSnapCase(snapi + 1, snapt + 1, entries(entry(snapi + 1, snapt)), snapi + 1),
                StableToWithSnapCase(snapi, snapt + 1, entries(entry(snapi + 1, snapt)), snapi + 1),
                StableToWithSnapCase(snapi - 1, snapt + 1, entries(entry(snapi + 1, snapt)), snapi + 1)
        );
        int size = tests.size();
        for (int i = 0; i < size; i++) {
            StableToWithSnapCase tt = tests.get(i);
            MemoryStorage s = MemoryStorage.newMemoryStorage();
            s.applySnapshot(Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(snapi).term(snapt).build()).build());
            RaftLog raftLog = RaftLog.newLog(s, RaftLogger.raftLogger);
            raftLog.append(tt.newEnts);
            raftLog.stableTo(tt.stablei, tt.stablet);
            if (raftLog.unstable.offset != tt.wunstable) {
                Assert.fail();
            }
        }
    }

    @Builder
    @Data
    @AllArgsConstructor
    static class CompactionCase {
        long lastIndex;
        long[] compact;
        int[] wleft;
        boolean wallow;
    }

    CompactionCase CompactionCase(
            long lastIndex,
            long[] compact,
            int[] wleft,
            boolean wallow
    ) {
        return new CompactionCase(lastIndex, compact, wleft, wallow);
    }

    @Test
    public void testCompaction() {
        List<CompactionCase> tests = Arrays.asList(
                CompactionCase(1000, new long[]{1001}, new int[]{-1}, false),
                CompactionCase(1000, new long[]{300, 500, 800, 900}, new int[]{700, 500, 200, 100}, true),
                CompactionCase(1000, new long[]{300, 299}, new int[]{700, -1}, false)
        );
        int size = tests.size();
        for (int i = 0; i < size; i++) {
            CompactionCase tt = tests.get(i);
            try {
                MemoryStorage storage = MemoryStorage.newMemoryStorage();
                for (int j = 1; j <= tt.lastIndex; j++) {
                    storage.append(entries(entry(j, 0)));
                }
                RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
                raftLog.matchCommit(tt.lastIndex, 0);
                raftLog.appliesTo(raftLog.committed);

                for (int j = 0; j < tt.compact.length; j++) {
                    try {
                        storage.compact(tt.compact[j]);
                    } catch (Exception e) {
                        if (tt.wallow) {
                            Assert.fail();
                        }
                        continue;
                    }
                    List<Raftpb.Entry> entries = raftLog.allEntries();
                    if (Util.len(entries) != tt.wleft[j]) {
                        Assert.fail();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (tt.wallow) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testLogRestore() throws Exception {
        long index = 1000L;
        long term = 1000L;

        Raftpb.SnapshotMetadata snap = Raftpb.SnapshotMetadata.builder().index(index).term(term).build();
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(Raftpb.Snapshot.builder().metadata(snap).build());
        RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);

        if (Util.len(raftLog.allEntries()) != 0) {
            Assert.fail();
        }
        if (raftLog.committed != index) {
            Assert.fail();
        }
        if (raftLog.unstable.offset != index + 1) {
            Assert.fail();
        }
        long t = 0L;
        Exception err = null;
        try {
            t = raftLog.term(index);
        } catch (Exception e) {
            err = e;
        }
        if (mustTerm(t, err) != term) {
            Assert.fail();
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class IsOutOfBoundsCase {
        long lo;
        long hi;
        boolean wpanic;
        boolean wErrCompacted;
    }

    IsOutOfBoundsCase IsOutOfBoundsCase(
            long lo,
            long hi,
            boolean wpanic,
            boolean wErrCompacted
    ) {
        return new IsOutOfBoundsCase(lo, hi, wpanic, wErrCompacted);
    }

    @Test
    public void testIsOutOfBounds() throws Exception {
        long offset = 100;
        long num = 100;

        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(offset).build()).build());
        RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
        for (int i = 1; i <= num; i++) {
            raftLog.append(entries(entry(i + offset, 0)));
        }
        long first = offset + 1;
        List<IsOutOfBoundsCase> tests = Arrays.asList(
                IsOutOfBoundsCase(first - 2, first + 1,
                        false, true),
                IsOutOfBoundsCase(first - 1, first + 1,
                        false, true),
                IsOutOfBoundsCase(first, first,
                        false, false),

                IsOutOfBoundsCase(first + num / 2, first + num / 2,
                        false, false),
                IsOutOfBoundsCase(first + num - 1, first + num - 1,
                        false, false),
                IsOutOfBoundsCase(first + num, first + num,
                        false, false),

                IsOutOfBoundsCase(first + num, first + num + 1,
                        true, false),
                IsOutOfBoundsCase(first + num + 1, first + num + 1,
                        true, false)
        );

        for (IsOutOfBoundsCase tt : tests) {
            try {
                Exception err = raftLog.mustCheckOutOfBounds(tt.lo, tt.hi);
                if (tt.wErrCompacted && err != Storage.ErrCompacted) {
                    Assert.fail();
                }
                if (!tt.wErrCompacted && err != null) {
                    Assert.fail();
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (tt.wpanic) {
                    Assert.fail();
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    @Builder
    static class TermCase {
        long index;
        long w;
    }

    TermCase TermCase(long index, long w) {
        return new TermCase(index, w);
    }

    @Test
    public void testTerm() throws Exception {
        long i = 0L;
        long offset = 100;
        long num = 100;

        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder()
                .index(offset)
                .term(1)
                .build()).build());
        RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
        for (i = 1; i < num; i++) {
            raftLog.append(entries(entry(offset + i, i)));
        }
        List<TermCase> tests = Arrays.asList(TermCase(offset - 1, 0),
                TermCase(offset, 1),
                TermCase(offset + num / 2, num / 2),
                TermCase(offset + num - 1, num - 1),
                TermCase(offset + num, 0));
        for (TermCase tt : tests) {
            Exception err = null;
            long l = 0L;
            try {
                l = raftLog.term(tt.index);
            } catch (Exception e) {
                err = e;
            }
            long term = mustTerm(l, err);
            if (term != tt.w) {
                Assert.fail();
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class TermWithUnstableSnapshotCase {
        long index;
        long w;
    }

    TermWithUnstableSnapshotCase TermWithUnstableSnapshotCase(
            long index,
            long w
    ) {
        return new TermWithUnstableSnapshotCase(index, w);
    }

    @Test
    public void testTermWithUnstableSnapshot() throws Exception {
        long storagesnapi = 100;
        long unstablesnapi = storagesnapi + 5;

        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder()
                .index(storagesnapi)
                .term(1)
                .build()).build());
        RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
        raftLog.restore(Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder()
                .index(unstablesnapi).term(1).build()).build());

        List<TermWithUnstableSnapshotCase> tests = Arrays.asList(
                TermWithUnstableSnapshotCase(storagesnapi, 0),
                TermWithUnstableSnapshotCase(storagesnapi + 1, 0),
                TermWithUnstableSnapshotCase(storagesnapi - 1, 0)
        );

        for (TermWithUnstableSnapshotCase tt : tests) {
            Exception err = null;
            long l = 0L;
            try {
                l = raftLog.term(tt.index);
            } catch (Exception e) {
                err = e;
            }
            long term = mustTerm(l, err);
            if (term != tt.w) {
                Assert.fail();
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class SliceCase {
        long from;
        long to;
        long limit;

        List<Raftpb.Entry> w;
        boolean wpanic;
    }

    SliceCase SliceCase(
            long from,
            long to,
            long limit,

            List<Raftpb.Entry> w,
            boolean wpanic
    ) {
        return new SliceCase(from, to, limit, w, wpanic);
    }

    @Test
    public void testSlice() throws Exception {
        long i = 0;
        long offset = 100;
        long num = 100;
        long last = offset + num;
        long half = offset + num / 2;
        Raftpb.Entry halfe = entry(half, half);

        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder()
                .index(offset)
                .build()).build());
        for (i = 1; i < num / 2; i++) {
            storage.append(entries(entry(offset + i, offset + i)));
        }
        RaftLog raftLog = RaftLog.newLog(storage, RaftLogger.raftLogger);
        for (i = num / 2; i < num; i++) {
            raftLog.append(entries(entry(offset + i, offset + i)));
        }

        List<SliceCase> tests = Arrays.asList(
                SliceCase(offset - 1, offset + 1, Raft.noLimit, null, false),
                SliceCase(offset, offset + 1, Raft.noLimit, null, false),
                SliceCase(half - 1, half + 1, Raft.noLimit, entries(entry(half - 1, half - 1), entry(half, half)), false),
                SliceCase(half, half + 1, Raft.noLimit, entries(entry(half, half)), false),
                SliceCase(last - 1, last, Raft.noLimit, entries(entry(last - 1, last - 1)), false),
                SliceCase(last, offset + 1, Raft.noLimit, null, true),

                SliceCase(half - 1, half + 1, 0, entries(entry(half - 1, half - 1)), false),
                SliceCase(half - 1, half + 1, Util.sizeOf(halfe) + 1, entries(entry(half - 1, half - 1)), false),
                SliceCase(half - 2, half + 1, Util.sizeOf(halfe) + 1, entries(entry(half - 2, half - 2)), false),
                SliceCase(half - 1, half + 1, Util.sizeOf(halfe) * 2, entries(entry(half - 1, half - 1), entry(half, half)), false),
                SliceCase(half - 1, half + 2, Util.sizeOf(halfe) * 3, entries(entry(half - 1, half - 1), entry(half, half), entry(half + 1, half + 1)), false),
                SliceCase(half, half + 2, Util.sizeOf(halfe), entries(entry(half, half)), false),
                SliceCase(half, half + 2, Util.sizeOf(halfe) * 2, entries(entry(half, half), entry(half + 1, half + 1)), true)
        );
        i = 0;
        for (; i < tests.size(); ++i) {
            SliceCase tt = tests.get((int) i);
            List<Raftpb.Entry> g = null;
            Exception err = null;
            try {
                g = raftLog.slice(tt.from, tt.to, tt.limit);
            } catch (Exception e) {
                e.printStackTrace();
                err = e;
            }
            if (tt.from <= offset && err != Storage.ErrCompacted) {
                Assert.fail();
            }
            if (tt.from > offset && err != null) {
                Assert.fail();
            }
            if (!(g == tt.w || g.equals(tt.w))) {
                Assert.fail();
            }
        }

    }

    public long mustTerm(long term, Exception err) {
        if (err != null) {
            Util.panic(err);
        }
        return term;
    }
}
