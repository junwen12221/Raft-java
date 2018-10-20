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

import java.nio.Buffer;
import java.util.ArrayList;
@Data
@Builder
public class Process {
    long match, next;
    ProgressStateType state = ProgressStateType.ProgressStateProbe;

    boolean paused;
    boolean recentActive;

    long pendingSnapshot;

    Inflights ins;
    int size;

    boolean isLeader;


    enum ProgressStateType {
        ProgressStateProbe, ProgressStateReplicate, ProgressStateSnapshot;
    }

    public void resetState(ProgressStateType state) {
        this.paused = false;
        this.pendingSnapshot = 0L;
        this.state = state;
        this.ins.reset();
    }

    public void becomeProbe() {
        if (this.state == ProgressStateType.ProgressStateSnapshot) {
            long pendingSnapshot = this.pendingSnapshot;
            this.resetState(ProgressStateType.ProgressStateSnapshot);
            this.next = Math.max(this.match + 1, pendingSnapshot + 1);
        } else {
            this.resetState(ProgressStateType.ProgressStateProbe);
            this.next = this.match + 1;
        }
    }
    public void becomeReplicate() {
        this.resetState(ProgressStateType.ProgressStateReplicate);
        this.next = this.match +1;
    }
    public void becomeSnapshot(long snapshoti){
        this.resetState(ProgressStateType.ProgressStateSnapshot);
        this.pendingSnapshot = snapshoti;
    }
    public boolean maybeUpdate(long n){
        boolean updated = false;
        if (this.match <n){
            this.match = n;
            updated = true;
            this.resume();
        }
        if (this.next < n+1){
            this.next = n+1;
        }
        return updated;
    }

    public boolean maybeDecrTo(long rejected,long last){
        if (this.state == ProgressStateType.ProgressStateReplicate){
            if (rejected <= this.match){
                return false;
            }
            this.next = this.match +1;
            return true;
        }
        if (this.next-1 != rejected){
            return false;
        }
        if ((this.next = Math.min(rejected,last+1))<1){
            this.next = 1;
        }
        this.resume();
        return true;
    }
    public void optimisticUpdate(long n){
        this.next = n+1;
    }

    public void pause() {
        this.paused = true;
    }

    public void resume() {
        this.paused = false;
    }

    public void snapshotFailure(){
        this.pendingSnapshot = 0L;
    }
    public boolean needSnapshotAbort(){
        return this.state == ProgressStateType.ProgressStateSnapshot
                &&
                this.match >= this.pendingSnapshot;
    }

    public boolean isPaused() {
        switch (state) {
            case ProgressStateProbe:
                return this.paused;
            case ProgressStateReplicate:
                return ins.full();
            case ProgressStateSnapshot:
                return true;
            default:
                throw new RuntimeException("unexpected state");
        }
    }

}
