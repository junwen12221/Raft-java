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

import java.util.Arrays;
import java.util.List;

public class Util {
    public static  <T> int len(List<T> list){
        if (list==null)return 0;
        return list.size();
    }
    public static boolean isHardStateEqual(Raftpb.HardState a, Raftpb.HardState b){
        return a.getTerm() == b.getTerm() && a.getVote() == b.getVote()
                && a.getCommit() == b.getCommit();
    }
    public static boolean isEmpty(Raftpb.HardState st){
        return isHardStateEqual(st, Node.emptyState);
    }
    public static  boolean isEmptySnap(Raftpb.Snapshot sp){
        return sp.getMetadata().getIndex() == 0;
    }
    public static void panic(String tmp, Object... args) {
        String value = String.format(tmp, args);
        System.out.println(value);
        throw new RuntimeException(value);
    }
    public static void panic(Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
    }
    public static void panic(Object... args) {
        String value = Arrays.deepToString(args);
        System.out.println(value);
        throw new RuntimeException(value);
    }

    public static int sizeOf(Raftpb.Entry entry){
        int size = entry == null?0:entry.getData()==null?0:entry.getData().length;
        return 20+size;
    }

    public static List<Raftpb.Entry> limitSize(List<Raftpb.Entry> ents, long maxSize){
        if (ents== null||ents.isEmpty())return ents;
        else {
            long size = sizeOf(ents.get(0));
            int limit=1;
            int entSize = ents.size();
            for (;limit<entSize;limit++){
               size+= sizeOf(ents.get(limit));
               if (size>maxSize){
                   break;
               }
            }
            return ents.subList(0,limit);
        }
    }
    public static <T> T[] append(T[] first, T[] second) {
        T[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    public static  boolean isLocalMsg(Raftpb.MessageType msgt){
        switch (msgt){
            case MsgHup:
            case MsgBeat:
            case MsgUnreachable:
            case MsgSnapStatus:
            case MsgCheckQuorum:
                return true;
                default:return false;
        }
    }

    public static Raftpb.MessageType voteRespMsgType(Raftpb.MessageType msgt){
        switch (msgt){
            case MsgVote:return Raftpb.MessageType.MsgVoteResp;
            case MsgPreVote:return Raftpb.MessageType.MsgPreVoteResp;
            default:
                Util.panic("not a vote message: %s", msgt);
        }
        return null;
    }

}
