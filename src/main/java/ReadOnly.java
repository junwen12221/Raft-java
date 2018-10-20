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

import java.util.*;

@Data
public class ReadOnly {
    ReadOnlyOption option;
    Map<String,ReadIndexStatus> pendingReadIndex;
    ArrayList<String> readIndexQueue = new  ArrayList<>();

    public ReadOnly(ReadOnlyOption option) {
        this.option = option;
        this.pendingReadIndex = new HashMap<>();
    }

    public void addRequest(long index, Raftpb.Message m){
        Raftpb.Entry entry = m.getEntries().get(0);
        String ctx = new String(entry.getData());
        boolean ok = this.pendingReadIndex.containsKey(ctx);
        if (ok){
            return;
        }
        ReadIndexStatus readIndexStatus = new ReadIndexStatus();
        readIndexStatus.setIndex(index);
        readIndexStatus.setAcks(new HashMap<>());
        readIndexStatus.setReq(m);
        this.pendingReadIndex.put(ctx,readIndexStatus);
        this.readIndexQueue.add(ctx);
    }

    public List<ReadIndexStatus> advance(Raftpb.Message m){
        int i = 0;
        boolean found  =false;
        String ctx = new String(m.getContext());
        ArrayList<ReadIndexStatus> rss = new ArrayList<>();
        for (String okctx : this.readIndexQueue) {
            ++i;
            ReadIndexStatus rs = this.pendingReadIndex.get(okctx);
            if (rs == null){
                Util.panic("cannot find corresponding read state from pending map");
            }
            rss.add(rs);
            if (okctx.equals(ctx)){
                found = true;
                break;
            }
        }
        if (found){
            this.readIndexQueue.subList(i,this.readIndexQueue.size());
            for (ReadIndexStatus rs : rss) {
                this.pendingReadIndex.remove(new String(rs.getReq().getEntries().get(0).getData()));
            }
            return rss;
        }
        return null;
    }
   public  String  lastPendingRequestCtx(){
        if (this.readIndexQueue.size() == 0){
            return "";
        }
        return this.readIndexQueue.get(this.readIndexQueue.size()-1);
   }
   @Data
   @Builder
    public   static class ReadState{
        long index;
        byte[] requestCtx;
    }
    @Data
    public static class ReadIndexStatus{
        Raftpb.Message req;
        long index;
        Map<Long,Boolean> acks;

    }

    public int recvAck(Raftpb.Message m){
        ReadIndexStatus status = this.pendingReadIndex.get(new String(m.getContext()));
        if (status == null)return 0;
        status.acks.put(m.getFrom(),Boolean.TRUE);
        return status.acks.size() +1;
    }
}
