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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/***
 * cjw
 */
@EqualsAndHashCode
@ToString
public class MemoryStorage implements Storage {

    final ReentrantLock lock = new ReentrantLock();
    Raftpb.HardState hardState = Raftpb.HardState.builder().build();
    Raftpb.ConfState confState = Raftpb.ConfState.builder().nodes(new ArrayList<>()).leaders(new ArrayList<>()).build();
    Raftpb.Snapshot snapshot = Raftpb.Snapshot.builder().metadata(Raftpb.SnapshotMetadata.builder().confState(Raftpb.ConfState.builder().build()).build()).build();
    List<Raftpb.Entry> ents;

    public MemoryStorage(List<Raftpb.Entry> ents) {
        this.ents = ents;
    }

    public static MemoryStorage newMemoryStorage() {
        ArrayList<Raftpb.Entry> array = new ArrayList<>();
        array.add(Raftpb.Entry.builder().build());
        return new MemoryStorage(array);
    }

    public Raftpb.HardState initialHardState() {
        return this.hardState;
    }

    public Raftpb.ConfState initialConfState() {
        return this.snapshot.getMetadata().getConfState();
    }

    public void setHardState(Raftpb.HardState st)throws Exception {
        using(ms -> {
            ms.hardState = st;
        });
    }

    interface UsingLockFun<R> {
        R apply(MemoryStorage t) throws Exception;
    }
    interface UsingLockCon {
        void accept(MemoryStorage t) throws Exception;
    }
    private void using(UsingLockCon c) throws Exception {
        this.lock.lock();
        try {
            c.accept(this);
        } catch (Exception e) {
            throw e;
        } finally {
            this.lock.unlock();
        }
    }
    private <R> R using(UsingLockFun<R> c) throws Exception {
        this.lock.lock();
        try {
            return c.apply(this);
        } catch (Exception e) {
            throw e;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public State initialSate() {
        return State.builder().confState(this.confState).hardState(this.hardState).build();
    }

    public List<Raftpb.Entry> entries(long lo, long hi, long maxSize) throws Exception {
        return using(ms -> {
            long offset = ms.ents.get(0).getIndex();
            if (lo <= offset) {
                throw ErrCompacted;
            }
            if (hi > ms._lastIndex() + 1) {
                RaftLogger.raftLogger.panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex());
            }
            if (ms.ents.size() == 1) {
                throw ErrUnavailable;
            }
            List<Raftpb.Entry> ents = ms.ents.subList((int) (lo - offset), (int) (hi - offset));
            return Util.limitSize(ents, maxSize);
        });
    }

    public long term(long i) throws Exception {
        return using(ms -> {
            long offset = ms.ents.get(0).getIndex();
            if (i < offset) {
                throw ErrCompacted;
            }
            if ((i - offset) >= ms.ents.size()) {
                throw ErrUnavailable;
            }
            return ms.ents.get((int) (i - offset)).getTerm();
        });
    }

    void append( final List<Raftpb.Entry> ents) throws Exception {
        if (ents == null || ents.isEmpty()) return;
        using(ms -> {
            List<Raftpb.Entry> entries = ents;
            long first = ms._firstIndex();
            long last = entries.get(0).getIndex() + entries.size() - 1;

            if (last < first) {
                return ;
            }

            if (first>entries.get(0).getIndex()){
                entries = new ArrayList<>(entries.subList((int)(first-entries.get(0).getIndex()),entries.size()));
            }
            int offset =(int) (entries.get(0).getIndex() - ms.ents.get(0).getIndex());
            int len = ms.ents.size();
            if (len > offset){
                ms.ents = new ArrayList<>(ms.ents.subList(0,offset));
                ms.ents.addAll(entries);
            }else if (len == offset){
                ms.ents = new ArrayList<>(this.ents);
                ms.ents.addAll(entries);
            }else {
                RaftLogger.raftLogger.panicf("missing log entry [last: %d, append at: %d]",
                        ms.lastIndex(), entries.get(0).getIndex());
            }
            return ;
        });
    }
    @Override
    public void close() {

    }

    public long lastIndex() throws Exception {
        return using(MemoryStorage::_lastIndex);
    }

    public long firstIndex() throws Exception {
        return using(MemoryStorage::_firstIndex);
    }

    private long _firstIndex() {
        return this.ents.get(0).getIndex() + 1;
    }

    public Raftpb.Snapshot snapshot() throws Exception {
        return using(ms -> ms.snapshot);
    }

    public void compact(long compactIndex)throws Exception{
        using(ms->{
            long offset = ms.ents.get(0).getIndex();
            if (compactIndex<=offset){
                throw ErrCompacted;
            }
            if (compactIndex>ms._lastIndex()){
                RaftLogger.raftLogger.panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex());
            }
            int i =(int)(compactIndex - offset);
            ArrayList<Raftpb.Entry> ents = new ArrayList<>();
            Raftpb.Entry entry = ms.ents.get(0);
            Raftpb.Entry update = ms.ents.get(i);
            Raftpb.Entry build = Raftpb.Entry.builder().data(entry.getData()).term(update.getTerm()).index(update.getIndex()).type(entry.getType()).build();
            ents.add(build);
            ents.addAll(ms.ents.subList(i+1, ms.ents.size()));
            ms.ents = ents;
            return;
        });
    }

    public void applySnapshot(Raftpb.Snapshot snap)throws Exception{
        using(ms->{
            long msIndex = ms.snapshot.getMetadata().getIndex();
            long snapIndex = snap.getMetadata().getIndex();
            if (msIndex >= snapIndex){
                throw  ErrSnapOutOfDate;
            }
            ms.snapshot = snap;
            ms.ents = new ArrayList<>();
            ms.ents.add(Raftpb.Entry.builder().term(snap.getMetadata().getTerm()).index(snap.getMetadata().getIndex()).build());
        });
    }
    public Raftpb.Snapshot createSnapshot(long i, Raftpb.ConfState cs,byte[] data)throws Exception{
        return using(ms->{
            if (i<ms.snapshot.getMetadata().getIndex()){
                throw ErrSnapOutOfDate;
            }
            long offset = ms.ents.get(0).getIndex();
            if (i>ms.lastIndex()){

            }
            Raftpb.SnapshotMetadata metadata = ms.snapshot.getMetadata();
            metadata.setIndex(i);
            metadata.setTerm(ms.ents.get((int)(i-offset)).getTerm());
            if (cs != null){
                ms.snapshot.getMetadata().setConfState(cs);
            }
            ms.snapshot.setData(data);
            return ms.snapshot;
        });
    }
    private long _lastIndex() {
        return this.ents.get(0).getIndex() + this.ents.size() - 1;
    }
}
