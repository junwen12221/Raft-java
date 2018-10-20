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

/**
 * cjw
 */
public class UtilTest {
    Util.EntryFormatter testFormmatter = (s) -> new String(s).toUpperCase();

    public static void errorf(String format, String defaultFormatted) {
        Assert.fail(String.format(format, defaultFormatted));
    }

    @Test
    public void testDescribeEntry() {
        Raftpb.Entry entry = Raftpb.Entry.builder()
                .term(1).index(2).type(Raftpb.EntryType.Normal)
                .data("hello\\x00world".getBytes())
                .build();

        String defaultFormatted = Util.describeEntry(entry, null);
        if (defaultFormatted.equals("1/2 EntryNormal \"hello\\x00world\"")) {
            errorf("unexpected default output: %s", "1/2 EntryNormal \"hello\\x00world\"");
        }
        String customFormatted = Util.describeEntry(entry, testFormmatter);
        if (customFormatted.equals("1/2 EntryNormal \"hello\\x00world\"")) {
            errorf("unexpected custom output: %s", customFormatted);
        }
    }

    LimitSizeCase LimitSizeCase(long maxsize,
                                List<Raftpb.Entry> wentries) {
        return new LimitSizeCase(maxsize, wentries);
    }

    Raftpb.Entry entry(int index, int term) {
        return StorageTest.entry(index, term);
    }

    @Test
    public void testLimitSize() {
        List<Raftpb.Entry> ents = Arrays.asList(
                Raftpb.Entry.builder().index(4).term(4).build(),
                Raftpb.Entry.builder().index(5).term(5).build(),
                Raftpb.Entry.builder().index(6).term(6).build()
        );
        List<LimitSizeCase> tests = Arrays.asList(
                LimitSizeCase(Long.MAX_VALUE, Arrays.asList(entry(4, 4), entry(5, 5), entry(6, 6))),
                LimitSizeCase(0, Arrays.asList(entry(4, 4))),
                LimitSizeCase(Util.sizeOf(ents.get(0)) + Util.sizeOf(ents.get(1)),
                        Arrays.asList(entry(4, 4), entry(5, 5))),
                LimitSizeCase(Util.sizeOf(ents.get(0)) + Util.sizeOf(ents.get(1)) + Util.sizeOf(ents.get(2)) / 2,
                        Arrays.asList(entry(4, 4), entry(5, 5))),
                LimitSizeCase(Util.sizeOf(ents.get(0)) + Util.sizeOf(ents.get(1)) + Util.sizeOf(ents.get(2)) - 1,
                        Arrays.asList(entry(4, 4), entry(5, 5))),
                LimitSizeCase(Util.sizeOf(ents.get(0)) + Util.sizeOf(ents.get(1)) + Util.sizeOf(ents.get(2)),
                        Arrays.asList(entry(4, 4), entry(5, 5), entry(6, 6)))
        );
        int i = 0;
        for (LimitSizeCase tt : tests) {
            ++i;
            if (!Util.limitSize(ents, tt.getMaxsize()).equals(tt.getWentries())) {
                Assert.fail(String.format("#%d: entries = %s, want %s", i, Util.limitSize(ents, tt.maxsize).toString(), tt.wentries.toString()));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    static class LimitSizeCase {
        long maxsize;
        List<Raftpb.Entry> wentries;
    }
}
