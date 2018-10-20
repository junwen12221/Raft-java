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

import java.util.List;

/***
 * cjw
 */
@Data
@Builder
public class Config {
    long id;
    List<Long> peers;
    List<Long> leaders;
    int electionTick;
    int heartbeatTick;
    Storage storage;
    long applied;
    long maxSizePerMsg;
    int maxInflightMsgs;
    boolean checkQuorum;
    boolean preVote;
    ReadOnlyOption readOnlyOption;
    RaftLog logger;
    boolean disableProposalForwarding;


    public void validate() throws Exception {
        if (id == 0L) {
            throw new RuntimeException("cannot use none as id");
        }
        if (heartbeatTick <= 0L) {
            throw new RuntimeException("heartbeat tick must be greater than 0");
        }
        if (electionTick <= heartbeatTick) {
            throw new RuntimeException("election tick must be greater than heartbeat tick");
        }
        if (storage == null) {
            throw new RuntimeException("storage cannot be nil");
        }
        if (maxInflightMsgs <= 0L) {
            throw new RuntimeException("max inflight messages must be greater than 0");
        }
        if (logger == null) {

        }

        if (this.readOnlyOption == ReadOnlyOption.ReadOnlyLeaseBased && ! checkQuorum){
            throw new RuntimeException("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased");
        }


    }

}
