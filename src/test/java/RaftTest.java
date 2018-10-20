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
import lombok.Value;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RaftTest {

    /**
     * @finished
     */
    @Test
    public void testProgressBecomeProbe() {
        long match = 1;

        List<ProgressBecomeProbeCase> tests = Arrays.asList(
                ProgressBecomeProbeCase(Process.builder()
                                .state(Process.ProgressStateType.ProgressStateReplicate)
                                .match(match)
                                .next(5)
                                .ins(Inflights.newInflights(256))
                                .build(),
                        2),
                // snapshot finish
                ProgressBecomeProbeCase(Process.builder()
                                .state(Process.ProgressStateType.ProgressStateSnapshot)
                                .match(match)
                                .next(5)
                                .pendingSnapshot(10)
                                .ins(Inflights.newInflights(256))
                                .build(),
                        11),
                // snapshot failure
                ProgressBecomeProbeCase(Process.builder()
                                .state(Process.ProgressStateType.ProgressStateSnapshot)
                                .match(match)
                                .next(5)
                                .pendingSnapshot(0)
                                .ins(Inflights.newInflights(256))
                                .build(),
                        2)
        );
        for (int i = 0; i < tests.size(); i++) {
            ProgressBecomeProbeCase tt = tests.get(i);
            Process p = tt.getProcess();
            p.becomeProbe();
            if (p.getState() != Process.ProgressStateType.ProgressStateProbe) {
                RaftTestUtil.errorf("#%d: state = %s, want %s", i, p.getState(), Process.ProgressStateType.ProgressStateProbe);
            }
            if (p.getMatch() != match) {
                RaftTestUtil.errorf("#%d: match = %d, want %d", i, p.getMatch(), match);
            }

            if (p.getNext() != tt.wnext) {
                RaftTestUtil.errorf("#%d: next = %d, want %d", i, p.getNext(), tt.wnext);
            }
        }
    }

    @Value
    @Builder
    @AllArgsConstructor
    static class ProgressBecomeProbeCase {
        Process process;
        long wnext;
    }

    ProgressBecomeProbeCase ProgressBecomeProbeCase(
            Process process,
            long wnext
    ) {
        return new ProgressBecomeProbeCase(process, wnext);
    }
}
