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
