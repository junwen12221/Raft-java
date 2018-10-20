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
import lombok.Value;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/***
 * cjw
 */
public class StorageTest {

    @AllArgsConstructor
    static class ItemCase {
        public Long i = 0L;
        public Exception werr = null;
        public Long wterm = 0L;
        public Boolean wpanic = false;
    }

    ItemCase Case(long i, Exception werr, long wterm, Boolean wpanic) {
        return new ItemCase(i, werr, wterm, wpanic);
    }

    @Test
    public void TestStorageTerm() {
        List<Raftpb.Entry> ents = Arrays.asList(
                entry(3, 3),
                entry(4, 4),
                entry(5, 5)
        );

        List<ItemCase> tests = Arrays.asList(
                Case(2, Storage.ErrCompacted, 0, false),
                Case(3, null, 3, false),
                Case(4, null, 4, false),
                Case(5, null, 5, false),
                Case(6, Storage.ErrUnavailable, 0, false)
        );
        for (ItemCase tt : tests) {
            Storage s = new MemoryStorage(ents);
            try {
                long term = 0L;
                try {
                    term = s.term(tt.i);
                } catch (Exception e) {
                    if (e != tt.werr) {
                        Assert.fail();
                    }
                    if (term != tt.wterm) {
                        Assert.fail();
                    }
                }
            } catch (Exception e) {
                if (!tt.wpanic) {//todo check wpainc classfly exception
                    Assert.fail();
                }
            }
        }
    }

    @AllArgsConstructor
    static class EntryCase {
        long lo, hi, maxsize;
        Exception werr;
        List<Raftpb.Entry> wentries;
    }

    EntryCase EntryCase(long lo, long hi, long maxsize,
                        Exception werr,
                        List<Raftpb.Entry> wentries) {
        return new EntryCase(lo, hi, maxsize, werr, wentries);
    }

 public static   List<Raftpb.Entry> entries(Raftpb.Entry... entries) {
        return Arrays.asList(entries);
    }

    @Test
    public void testStorageEntries() {
        List<Raftpb.Entry> ents = Arrays.asList(
                entry(3, 3),
                entry(4, 4),
                entry(5, 5),
                entry(6, 6)
        );
        long maxLong = Long.MAX_VALUE;
        List<EntryCase> tests = Arrays.asList(
                EntryCase(2, 6, maxLong, Storage.ErrCompacted, null),
                EntryCase(3, 4, maxLong, Storage.ErrCompacted, null),
                EntryCase(4, 5, maxLong, null, entries(entry(4, 4))),
                EntryCase(4, 6, maxLong, null, entries(entry(4, 4), entry(5, 5))),
                EntryCase(4, 7, maxLong, null, entries(entry(4, 4), entry(5, 5), entry(6, 6))),
                EntryCase(4, 7, 0, null, entries(entry(4, 4))),
                EntryCase(4, 7, Util.sizeOf(ents.get(1)) + Util.sizeOf(ents.get(2)), null, entries(entry(4, 4),
                        entry(5, 5))),
                EntryCase(4, 7, Util.sizeOf(ents.get(1)) + Util.sizeOf(ents.get(2)) + Util.sizeOf(ents.get(3)) / 2, null, entries(entry(4, 4),
                        entry(5, 5))),
                EntryCase(4, 7, Util.sizeOf(ents.get(1)) + Util.sizeOf(ents.get(2)) + Util.sizeOf(ents.get(3)) - 1, null,
                        entries(entry(4, 4),
                                entry(5, 5))),
                EntryCase(4, 7, Util.sizeOf(ents.get(1)) + Util.sizeOf(ents.get(2)) + Util.sizeOf(ents.get(3)), null, entries(entry(4, 4),
                        entry(5, 5),
                        entry(6, 6)
                )));
        for (EntryCase tt : tests) {
            MemoryStorage s = new MemoryStorage(ents);
            List<Raftpb.Entry> entries = null;
            try {
                entries = s.entries(tt.lo, tt.hi, tt.maxsize);
            } catch (Exception err) {
                if (err != tt.werr) {
                    Assert.fail();
                }
            }
            if (!Objects.equals(entries, tt.wentries)) {
                Assert.fail();
            }
        }
    }

    @Test
    public void testStorageFirstIndex() throws Exception {
        List<Raftpb.Entry> entries = entries(entry(3, 3), entry(4, 4), entry(5, 5));
        MemoryStorage s = new MemoryStorage(entries);
        long first = 0L;
        try {
            first = s.firstIndex();
        } catch (Exception e) {
            Assert.fail();
        }
        if (first != 4) {
            Assert.fail();
        }
        s.compact(4);
        first = s.firstIndex();
        try {
            s.firstIndex();
        } catch (Exception e) {
            Assert.fail();
        }
        if (first != 5) {
            Assert.fail();
        }
    }

    @Builder
    @AllArgsConstructor
    static class IndexCase {
        public long i, windex, wterm;
        public Exception werr;
        public int wlen;
    }

    private IndexCase IndexCase(long i, Exception error, long windex, long wterm, int wlen) {
        return new IndexCase(i, windex, wterm, error, wlen);
    }


    @Test
    public void testStorageCompact() throws Exception {
        List<Raftpb.Entry> ents = Collections.unmodifiableList(entries(entry(3, 3), entry(4, 4), entry(5, 5)));
        List<IndexCase> tests = Arrays.asList(
                IndexCase(2, Storage.ErrCompacted, 3, 3, 3),
                IndexCase(3, Storage.ErrCompacted, 3, 3, 3),
                IndexCase(4, null, 4, 4, 2),
                IndexCase(5, null, 5, 5, 1)
        );

        for (IndexCase tt : tests) {
            MemoryStorage s = new MemoryStorage(ents);
            try {
                s.compact(tt.i);
            } catch (Exception err) {
                if (err != tt.werr) {
                    Assert.fail();
                }
            }
            if (s.ents.get(0).getIndex() != tt.windex) {
                Assert.fail();
            }
            if (s.ents.get(0).getTerm() != tt.wterm) {
                Assert.fail();
            }
            if (s.ents.size() != tt.wlen) {
                Assert.fail();
            }
        }

    }

    @Builder
    @Value
    static class SnapshotCase {
        long i;
        Exception error;
        Raftpb.Snapshot wsnap;
    }

    SnapshotCase SnapshotCase(long i, Exception error, Raftpb.Snapshot wsnap) {
        return SnapshotCase.builder().i(i).error(error).wsnap(wsnap).build();
    }

    @Test
    public void testStorageCreateSnapshot() {
        List<Raftpb.Entry> ents = entries(entry(3, 3), entry(4, 4), entry(5, 5));
        Raftpb.ConfState cs = Raftpb.ConfState.builder().nodes(Arrays.asList(1L, 2L, 3L)).build();
        byte[] data = "data".getBytes();
        Raftpb.Snapshot sp1 = Raftpb.Snapshot.builder().data(data).metadata(Raftpb.SnapshotMetadata.builder().index(4).term(4).confState(cs).build()).build();
        Raftpb.Snapshot sp2 = Raftpb.Snapshot.builder().data(data).metadata(Raftpb.SnapshotMetadata.builder().index(5).term(5).confState(cs).build()).build();
        List<SnapshotCase> tests = Arrays.asList(SnapshotCase(4, null, sp1), SnapshotCase(5, null, sp2));
        for (SnapshotCase tt : tests) {
            MemoryStorage s = new MemoryStorage(ents);
            Raftpb.Snapshot snap = null;
            try {
                snap = s.createSnapshot(tt.i, cs, data);
            } catch (Exception err) {
                if (err != tt.error) {
                    err.printStackTrace();
                    Assert.fail();
                }
            }
            if (!snap.equals(tt.wsnap)) {
                Assert.fail();
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class StorageEntries {
        List<Raftpb.Entry> entries;
        Exception werr;
        List<Raftpb.Entry> wentries;
    }

    StorageEntries StorageEntries(List<Raftpb.Entry> entries,
                                  Exception werr,
                                  List<Raftpb.Entry> wentries) {
        return new StorageEntries(entries, werr, wentries);
    }

    @Test
    public void testStorageAppend() {
        List<Raftpb.Entry> ents = entries(entry(3, 3), entry(4, 4), entry(5, 5));
        StorageEntries se1 = StorageEntries(entries(entry(3, 3), entry(4, 4), entry(5, 5)),
                null,
                entries(entry(3, 3), entry(4, 4), entry(5, 5)));

        StorageEntries se2 = StorageEntries(entries(entry(3, 3), entry(4, 6), entry(5, 6)),
                null,
                entries(entry(3, 3), entry(4, 6), entry(5, 6)));

        StorageEntries se3 = StorageEntries(entries(entry(3, 3), entry(4, 4), entry(5, 5),entry(6, 5)),
                null,
                entries(entry(3, 3), entry(4, 4), entry(5, 5),entry(6, 5)));

        StorageEntries se4 = StorageEntries(entries(entry(2, 3), entry(3, 3), entry(4, 5)),
                null,
                entries(entry(3, 3), entry(4, 5)));

        StorageEntries se5 = StorageEntries(entries(entry(4, 5)),
                null,
                entries(entry(3, 3), entry(4, 5)));

        StorageEntries se6 = StorageEntries(entries(entry(6, 5)),
                null,
                entries(entry(3, 3), entry(4, 4), entry(5, 5), entry(6, 5)));
        List<StorageEntries> tests = Arrays.asList(se1, se2, se3, se4, se5, se6);
        for (StorageEntries tt : tests) {
            MemoryStorage s = new MemoryStorage(ents);
            try {
                s.append(tt.entries);
            }catch (Exception e){
                if (e!=tt.werr){
                    e.printStackTrace();
                    Assert.fail();
                }
                if (!s.ents.equals(tt.wentries)){
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testStorageApplySnapshot(){
        Raftpb.ConfState cs = Raftpb.ConfState.builder().nodes(Arrays.asList(1L, 2L, 3L)).build();
        byte[] data = "data".getBytes();
        Raftpb.Snapshot s1 = Raftpb.Snapshot.builder()
                .data(data)
                .metadata(Raftpb.SnapshotMetadata.builder()
                        .index(4)
                        .term(4)
                        .confState(cs)
                        .build())
                .build();

        Raftpb.Snapshot s2 = Raftpb.Snapshot.builder()
                .data(data)
                .metadata(Raftpb.SnapshotMetadata.builder()
                        .index(3)
                        .term(3)
                        .confState(cs)
                        .build())
                .build();
        List<Raftpb.Snapshot> tests = Arrays.asList(s1, s2);
        MemoryStorage s = MemoryStorage.newMemoryStorage();
        int i = 0;
        Raftpb.Snapshot tt = tests.get(i);
        try{
            s.applySnapshot(tt);
        }catch (Exception e){
            if (e != null){
                Assert.fail();
            }
        }

        i = 1;
         tt = tests.get(i);
         try {
             s.applySnapshot(tt);
         }catch (Exception e){
             if (e!= Storage.ErrSnapOutOfDate){
                 Assert.fail();
             }
         }

    }

    public static Raftpb.Entry entry(long index, long term) {
        return Raftpb.Entry.builder()
                .index(index)
                .term(term)
                .build();
    }

}
