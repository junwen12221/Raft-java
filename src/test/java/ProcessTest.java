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


import org.junit.Assert;
import org.junit.Test;

public class ProcessTest {
    @Test
    public void testInflighsAdd(){
        Inflights in = new Inflights(10,new long[10]);
        for (int i = 0; i < 5; i++) {
            in.add(i);
        }
        Inflights wantIn = new Inflights(0, 5, 10, new long[]{
                0,1,2,3,4,0,0,0,0,0
        });

        if (!in.equals(wantIn)){
            Assert.fail("in = " + in +" want = "+wantIn);
        }
        for (int i = 5; i <10 ; i++) {
            in.add(i);
        }

        Inflights wantIn2 = new Inflights(0, 10, 10, new long[]{0,1, 2, 3, 4, 5, 6, 7, 8, 9});
        if (!in.equals(wantIn2)){
            Assert.fail();
        }
        Inflights in2 = new Inflights(5, 0, 10, new long[10]);
        for (int i = 0; i < 5; i++) {
            in2.add(i);
        }

        Inflights wantIn21 = new Inflights(5, 5, 10, new long[]{0, 0, 0, 0, 0, 0, 1, 2, 3, 4});

        if (! in2.equals(wantIn21)){
            Assert.fail();
        }
        for (int i = 5; i <10 ; i++) {
            in2.add(i);
        }

        Inflights wantIn22 = new Inflights(5, 10, 10, new long[]{5, 6, 7, 8, 9, 0, 1, 2, 3, 4});
        if (!in2.equals(wantIn22)){
            Assert.fail();
        }
    }
    @Test
    public void testInflightFreeTo(){
        Inflights in = Inflights.newInflights(10);
        for (int i = 0; i < 10; i++) {
            in.add(i);
        }
        in.freeTo(4);

        Inflights wantIn = new Inflights(5, 5, 10, new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        if (!in.equals(wantIn)){
            Assert.fail();
        }
        in.freeTo(8);

        Inflights wantIn2 = new Inflights(9, 1, 10, new
                long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        if (!in.equals(wantIn2)){
            Assert.fail();
        }
        for (int i = 10; i < 15; i++) {
            in.add(i);
        }
        in.freeTo(12);

        Inflights wantIn3 = new Inflights(3, 2, 10, new long[]{10, 11, 12, 13, 14, 5, 6, 7, 8, 9});
        if (!in.equals(wantIn3)){
            Assert.fail();
        }
        in.freeTo(14);

        Inflights wantIn4 = new Inflights(0, 0, 10, new long[]{10, 11, 12, 13, 14, 5, 6, 7, 8, 9});

        if (!in.equals(wantIn4)){
            Assert.fail();
        }
    }
    @Test
    public void testInflightFreeFirstOne(){
        Inflights in = Inflights.newInflights(10);
        for (int i = 0; i < 10; i++) {
            in.add(i);
        }
        in.freeFirstOne();

        Inflights wantIn = new Inflights(1, 9, 10, new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        if (!in.equals(wantIn)){
            Assert.fail();
        }
    }

}
