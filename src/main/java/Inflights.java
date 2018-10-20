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

public class Inflights {
    int start, count, size;
    long[] buffer;

    public Inflights(int size, long[] buffer) {
        this.size = size;
        this.buffer = buffer;
    }

    public Inflights(int start, int count, int size, long[] buffer) {
        this.start = start;
        this.count = count;
        this.buffer = buffer;
        this.size = size;
    }

    public static Inflights newInflights(int size) {
        return new Inflights(size,new long[size]);
    }
    public void add(long inflight) {
        if (this.full()){
            Util.panic("cannot add into a full inflights");
        }
        int next = this.start +this.count;
        int size = this.size;
        if (next >= size){
            next -= size;
        }
        if (next >= this.buffer.length){
            this.growBuf();
        }
        this.buffer[next] = inflight;
        this.count++;
    }

    public void freeTo(long to) {
        if (this.count == 0 || to < this.buffer[this.start]) {
            return;
        }
        int idx = this.start;
        int i = 0;
        for (; i < this.count; ++i) {
            if (to < this.buffer[idx]) {
                break;
            }
            int size = this.size;
            if ((++idx) >= size) {
                idx -= size;
            }
        }
        this.count -= i;
        this.start = idx;
        if (this.count == 0) {
            this.start = 0;
        }
    }

    public void growBuf() {
        int newSize = this.buffer.length * 2;
        if (newSize == 0) {
            newSize = 1;
        } else if (newSize > this.size) {
            newSize = this.size;
        }
        long[] newBuffer = new long[newSize];
        System.arraycopy(this.buffer, 0, newBuffer, 0, newSize);
        this.buffer = newBuffer;
    }

    public void freeFirstOne() {
        this.freeTo(this.buffer[this.start]);
    }

    public boolean full() {
        return this.count == this.size;
    }

    public void reset() {
        this.count = 0;
        this.start = 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Inflights)) return false;

        Inflights inflights = (Inflights) o;

        if (start != inflights.start) return false;
        if (count != inflights.count) return false;
        if (size != inflights.size) return false;
        return Arrays.equals(buffer, inflights.buffer);
    }

    @Override
    public int hashCode() {
        int result = start;
        result = 31 * result + count;
        result = 31 * result + size;
        result = 31 * result + Arrays.hashCode(buffer);
        return result;
    }
}
