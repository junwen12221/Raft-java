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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Util {
    final static Logger logger = LoggerFactory.getLogger(Util.class);

    /**
     * @finished
     * @param stateType
     * @return
     */
    public static byte[] marshalJSON(Raft.StateType stateType) {
        return stateType.toString().getBytes();
    }

    /**
     * @finished
     * @param list
     * @param <T>
     * @return
     */
    public static <T> int len(List<T> list) {
        if (list == null) return 0;
        return list.size();
    }

    /**
     * @finished
     * @param a
     * @param b
     * @return
     */
    public static boolean isHardStateEqual(Raftpb.HardState a, Raftpb.HardState b) {
        return a.getTerm() == b.getTerm() && a.getVote() == b.getVote()
                && a.getCommit() == b.getCommit();
    }

    /**
     * @finished
     * @param st
     * @return
     */
    public static boolean isEmpty(Raftpb.HardState st) {
        return isHardStateEqual(st, Node.emptyState);
    }

    /**
     * @finished
     * @param sp
     * @return
     */
    public static boolean isEmptySnap(Raftpb.Snapshot sp) {
        return sp.getMetadata().getIndex() == 0;
    }

    /**
     * @finished
     * @param tmp
     * @param args
     */
    public static void panic(String tmp, Object... args) {
        String value = String.format(tmp, args);
        System.out.println(value);
        throw new RuntimeException(value);
    }

    /**
     * @finished
     * @param e
     */
    public static void panic(Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
    }

    /**
     * @finished
     * @param args
     */
    public static void panic(Object... args) {
        String value = Arrays.deepToString(args);
        System.out.println(value);
        throw new RuntimeException(value);
    }

    /**
     * @finished
     * @param entry
     * @return
     */
    public static int sizeOf(Raftpb.Entry entry) {
        int size = entry == null ? 0 : entry.getData() == null ? 0 : entry.getData().length;
        return 20 + size;
    }

    /**
     * @finished
     * @param ents
     * @param maxSize
     * @return
     */
    public static List<Raftpb.Entry> limitSize(List<Raftpb.Entry> ents, long maxSize) {
        if (ents == null || ents.isEmpty()) return ents;
        else {
            long size = sizeOf(ents.get(0));
            int limit = 1;
            int entSize = ents.size();
            for (; limit < entSize; limit++) {
                size += sizeOf(ents.get(limit));
                if (size > maxSize) {
                    break;
                }
            }
            return ents.subList(0, limit);
        }
    }

    /**
     * @finished
     * @param first
     * @param second
     * @param <T>
     * @return
     */
    public static <T> T[] append(T[] first, T[] second) {
        T[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    /**
     * @finished
     * @param msgt
     * @return
     */
    public static boolean isLocalMsg(Raftpb.MessageType msgt) {
        switch (msgt) {
            case MsgHup:
            case MsgBeat:
            case MsgUnreachable:
            case MsgSnapStatus:
            case MsgCheckQuorum:
                return true;
            default:
                return false;
        }
    }

    /**
     * @finished
     * @param msgt
     * @return
     */
    public static Raftpb.MessageType voteRespMsgType(Raftpb.MessageType msgt) {
        switch (msgt) {
            case MsgVote:
                return Raftpb.MessageType.MsgVoteResp;
            case MsgPreVote:
                return Raftpb.MessageType.MsgPreVoteResp;
            default:
                Util.panic("not a vote message: %s", msgt);
        }
        return null;
    }

    /**
     * @finished
     * @param msgt
     * @return
     */
    public static boolean IsResponseMsg(Raftpb.MessageType msgt) {
        switch (msgt) {
            case MsgAppResp:
                break;
            case MsgVoteResp:
                break;
            case MsgUnreachable:
                break;
            case MsgHeartbeat:
                break;
            case MsgPreVoteResp:
                return true;
            default:
                return false;
        }
       return false;
    }

    /**
     * @finished
     */
    interface EntryFormatter{
        String apply(byte[] s);
    }

    /**
     * @finished
     * @param format
     * @param objects
     */
    public static void fprintf(String format,Object... objects){
        System.out.format(format+"\n",objects);
    }

    /**
     * @finished
     * @param m
     * @param formatter
     * @return
     */
    public static String DescribeMessage(Raftpb.Message m, EntryFormatter formatter) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(String.format("%x->%x %s Term:%d Log:%d/%d", m.getFrom(),
                m.getTo(), m.getType().toString(),m.getTerm()
                , m.getLogTerm(), m.getIndex()));
        if (m.isReject()){
            buffer.append(" Rejected");
            if (m.getRejectHint()!=0){
                buffer.append(String.format("(Hint:%d)", m.getRejectHint()));
            }
        }
        if (m.getCommit()!=0L){
            buffer.append(String.format(" Commit:%d", m.getCommit()));
        }
        if (len(m.getEntries())>0){
            buffer.append(Objects.toString(m.getEntries()));
        }
        return buffer.toString();
    }

    /**
     * @finished
     * @param e
     * @param f
     * @return
     */
    public static String describeEntry(Raftpb.Entry e,EntryFormatter f) {
        String formatted;
        try {
            if (e.getType() == Raftpb.EntryType.Normal && f != null) {
                formatted =   f.apply(e.getData());
            } else {
                formatted  = String.format("%s", Arrays.toString(e.getData()));
            }
            return String.format("%d/%d %s %s", e.getTerm(), e.getIndex(), e.getType(), formatted);
        }catch (Exception e1){
            e1.printStackTrace();
            throw new RuntimeException(e1);
        }
    }
}
