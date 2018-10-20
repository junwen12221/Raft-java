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

public class LogUnstableTest {
    public FirstIndexCase firstIndexCase(
            List<Raftpb.Entry> entries,
            long offset,
            Raftpb.Snapshot snap,
            boolean wok,
            long windex
    ) {
        return new FirstIndexCase(entries, offset, snap, wok, windex);
    }

    @Test
    public void testUnstableMaybeFirstIndex() {
        List<FirstIndexCase> tests = Arrays.asList(
                firstIndexCase(Arrays.asList(StorageTest.entry(5, 1)), 5, null, false, 0),
                firstIndexCase(Arrays.asList(), 0, null, false, 0),
                firstIndexCase(Arrays.asList(StorageTest.entry(5, 1)), 5, Raftpb.Snapshot.builder()
                        .metadata(Raftpb.SnapshotMetadata.builder()
                                .index(4)
                                .term(1).build()).build(), true, 5),
                firstIndexCase(Arrays.asList(), 5, Raftpb.Snapshot.builder()
                        .metadata(Raftpb.SnapshotMetadata.builder()
                                .index(4)
                                .term(1).build()).build(), true, 5)
        );
        for (int i = 0; i < tests.size(); i++) {
            FirstIndexCase tt = tests.get(i);
            Unstable u = Unstable.builder().entries(tt.entries).offset(tt.offset).snapshot(tt.snap).logger(RaftLogger.raftLogger).build();
            Unstable.MaybeResult maybeResult = u.maybeFirstIndex();
            if (maybeResult.isHasSnapshot() != tt.wok) {
                Assert.fail();
            }
            if (maybeResult.getValue() != tt.windex) {
                Assert.fail();
            }
        }
    }

    private LastIndexCase LastIndexCase(
            List<Raftpb.Entry> entries,
            long offset,
            Raftpb.Snapshot snap,
            boolean wok,
            long windex
    ) {
        return new LastIndexCase(entries, offset, snap, wok, windex);
    }

    @Test
    public void testMaybeLastIndex() {
        List<LastIndexCase> tests = Arrays.asList(
                LastIndexCase(Arrays.asList(StorageTest.entry(5, 1)), 5, null, true, 5),
                LastIndexCase(Arrays.asList(StorageTest.entry(5, 1)), 5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(),
                        true, 5),
                LastIndexCase(Arrays.asList(), 5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(),
                        true, 4),
                LastIndexCase(Arrays.asList(), 0,
                        null,
                        false, 0)
        );
        for (int i = 0; i < tests.size(); i++) {
            LastIndexCase tt = tests.get(i);
            Unstable u = Unstable.builder().entries(tt.entries).offset(tt.offset).snapshot(tt.snap).logger(RaftLogger.raftLogger).build();
            Unstable.MaybeResult maybeResult = u.maybeLastIndex();
            if (maybeResult.isHasSnapshot() != tt.wok) {
                Assert.fail();
            }
            if (maybeResult.getValue() != tt.windex) {
                Assert.fail();
            }
        }

    }

    private MaybeTermCase MaybeTermCase(
            List<Raftpb.Entry> entries,
            long offset,
            Raftpb.Snapshot snap,
            long index,
            boolean wok,
            long windex
    ) {
        return new MaybeTermCase(entries, offset, snap, index, wok, windex);
    }

    @Test
    public void testUnstableMaybeTerm() {
        List<MaybeTermCase> tests = Arrays.asList(
                MaybeTermCase(Arrays.asList(StorageTest.entry(5, 1)), 5, null,
                        5,
                        true, 1),
                MaybeTermCase(Arrays.asList(StorageTest.entry(5, 1)), 5, null,
                        6,
                        false, 0),
                MaybeTermCase(Arrays.asList(StorageTest.entry(5, 1)), 5, null,
                        4,
                        false, 0),
                MaybeTermCase(Arrays.asList(StorageTest.entry(5, 1)),
                        5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(),
                        5,
                        true, 1),
                MaybeTermCase(Arrays.asList(StorageTest.entry(5, 1)),
                        5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(),
                        6,
                        false, 0),
                MaybeTermCase(Arrays.asList(StorageTest.entry(5, 1)),
                        5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).
                                build()).build(), 4,
                        true, 1),
                MaybeTermCase(Arrays.asList(StorageTest.entry(5, 1)),
                        5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1
                        ).build()).build(), 3,
                        false, 0),
                MaybeTermCase(Arrays.asList(),
                        5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1)
                                .build()).build(), 5,
                        false, 0)
                ,
                MaybeTermCase(Arrays.asList(),
                        5,
                        Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1)
                                .build()).build(), 4,
                        true, 1),
                MaybeTermCase(Arrays.asList(),
                        0,
                        null, 5,
                        false, 0)
        );
        for (int i = 0; i < tests.size(); i++) {
            MaybeTermCase tt = tests.get(i);
            Unstable u = Unstable.builder().entries(tt.entries).offset(tt.offset).snapshot(tt.snap).logger(RaftLogger.raftLogger).build();
            Unstable.MaybeResult maybeResult = u.maybeTerm(tt.index);
            if (maybeResult.isHasSnapshot() != tt.wok) {
                Assert.fail();
            }
            if (maybeResult.getValue() != tt.wterm) {
                Assert.fail();
            }
        }

    }

    @Test
    public void testUnstableRestore() {
        Unstable u = Unstable.builder().entries(Arrays.asList(StorageTest.entry(5, 1)))
                .offset(5).snapshot(Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build())
                .logger(RaftLogger.raftLogger).build();

        Raftpb.Snapshot s = Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build();
        u.restore(s);
        if (u.getOffset() != s.getMetadata().getIndex() + 1) {
            Assert.fail();
        }
        if (u.entries.size() != 0) {
            Assert.fail();
        }
        if (!u.getSnapshot().equals(s)) {
            Assert.fail();
        }

    }

    public UnstableStableTo UnstableStableTo(
            List<Raftpb.Entry> entries,
            long offset,
            Raftpb.Snapshot snap,
            long index, long term,

            long woffset,
            int wlen
    ) {
        return new UnstableStableTo(entries, offset, snap, index, term, woffset, wlen);
    }

    @Test
    public void testUnstableStableTo() {
        List<UnstableStableTo> tests = Arrays.asList(
                UnstableStableTo(Arrays.asList(), 0, null, 5, 1, 0, 0),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(1).build()), 5, null, 5, 1, 6, 0),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(1).build(), Raftpb.Entry.builder().index(6).term(1).build()),
                        5, null, 5, 1, 6, 1),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(6).term(2).build()),
                        6, null, 6, 1, 6, 1),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(1).build()),
                        5, null, 4, 1, 5, 1),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(1).build()),
                        5, null, 4, 2, 5, 1),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(1).build()),
                        5, Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(), 5, 1, 6, 0),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(1).build(), StorageTest.entry(6, 1)),
                        5, Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(), 5, 1, 6, 1),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(6).term(2).build()),
                        6, Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(), 6, 1, 6, 1),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(1).build()),
                        5, Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(1).build()).build(), 4, 1, 5, 1),
                UnstableStableTo(Arrays.asList(Raftpb.Entry.builder().index(5).term(2).build()),
                        5, Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().index(4).term(2).build()).build(), 4, 1, 5, 1)
        );

        for (UnstableStableTo tt : tests) {
            Unstable u = Unstable.builder().entries(tt.entries).offset(tt.offset).snapshot(tt.snap).logger(RaftLogger.raftLogger).build();
            u.stableTo(tt.index, tt.term);
            if (u.offset != tt.woffset) {
                Assert.fail();
            }
            if (u.entries.size() != tt.wlen) {
                Assert.fail();
            }
        }

    }

    @AllArgsConstructor
    @Builder
    @Data
    static class UnstableTruncateAndAppendCase {
        List<Raftpb.Entry> entries;
        long offset;
        Raftpb.Snapshot snap;
        List<Raftpb.Entry> toappend;

        long woffset;
        List<Raftpb.Entry> wentries;
    }

    public UnstableTruncateAndAppendCase UnstableTruncateAndAppendCase(
            List<Raftpb.Entry> entries,
            long offset,
            Raftpb.Snapshot snap,
            List<Raftpb.Entry> toappend,

            long woffset,
            List<Raftpb.Entry> wentries
    ) {
        return new UnstableTruncateAndAppendCase(entries, offset, snap, toappend, woffset, wentries)
                ;
    }

    @Test
    public void testUnstableTruncateAndAppend() {
        List<UnstableTruncateAndAppendCase> tests = Arrays.asList(
                UnstableTruncateAndAppendCase(
                        Arrays.asList(StorageTest.entry(5, 1)), 5, null,
                        Arrays.asList(StorageTest.entry(6, 1), StorageTest.entry(7, 1)),
                        5, Arrays.asList(StorageTest.entry(5, 1), StorageTest.entry(6, 1), StorageTest.entry(7, 1))),
                UnstableTruncateAndAppendCase(
                        Arrays.asList(StorageTest.entry(5, 1)), 5, null,
                        Arrays.asList(StorageTest.entry(5, 2), StorageTest.entry(6, 2)),
                        5, Arrays.asList(StorageTest.entry(5, 2), StorageTest.entry(6, 2))),
                UnstableTruncateAndAppendCase(
                        Arrays.asList(StorageTest.entry(5, 1)), 5, null,
                        Arrays.asList(StorageTest.entry(4, 2), StorageTest.entry(5, 2),StorageTest.entry(6, 2)),
                        4, Arrays.asList(StorageTest.entry(4, 2),StorageTest.entry(5, 2), StorageTest.entry(6, 2))),
                UnstableTruncateAndAppendCase(
                        Arrays.asList(StorageTest.entry(5, 1)), 5, null,
                        Arrays.asList(StorageTest.entry(4, 2), StorageTest.entry(5, 2), StorageTest.entry(6, 2)),
                        4, Arrays.asList(StorageTest.entry(4, 2), StorageTest.entry(5, 2), StorageTest.entry(6, 2))),
                UnstableTruncateAndAppendCase(
                        Arrays.asList(StorageTest.entry(5, 1), StorageTest.entry(6, 1), StorageTest.entry(7, 1)), 5, null,
                        Arrays.asList(StorageTest.entry(6, 2)),
                        5, Arrays.asList(StorageTest.entry(5, 1), StorageTest.entry(6, 2))),
                UnstableTruncateAndAppendCase(
                        Arrays.asList(StorageTest.entry(5, 1), StorageTest.entry(6, 1), StorageTest.entry(7, 1)), 5, null,
                        Arrays.asList(StorageTest.entry(7, 2), StorageTest.entry(8, 2)),
                        5, Arrays.asList(StorageTest.entry(5, 1), StorageTest.entry(6, 1), StorageTest.entry(7, 2),
                                StorageTest.entry(8, 2)))
        );
        for (int i = 0; i < tests.size(); i++) {
            UnstableTruncateAndAppendCase tt = tests.get(i);
            Unstable u = Unstable.builder().entries(tt.entries).offset(tt.offset).snapshot(tt.snap).logger(RaftLogger.raftLogger).build();
            u.truncateAndAppend(tt.toappend);
            if (u.offset != tt.woffset) {
                Assert.fail();
            }
            if (!u.entries.equals(tt.wentries)) {
                Assert.fail();
            }
        }



    }

    @Data
    @Builder
    static class UnstableStableTo {
        List<Raftpb.Entry> entries;
        long offset;
        Raftpb.Snapshot snap;
        long index, term;

        long woffset;
        int wlen;
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class FirstIndexCase {
        List<Raftpb.Entry> entries;
        long offset;
        Raftpb.Snapshot snap;
        boolean wok;
        long windex;
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class LastIndexCase {
        List<Raftpb.Entry> entries;
        long offset;
        Raftpb.Snapshot snap;
        boolean wok;
        long windex;
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class MaybeTermCase {
        List<Raftpb.Entry> entries;
        long offset;
        Raftpb.Snapshot snap;
        long index;

        boolean wok;
        long wterm;
    }
}
