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

/***
 * cjw
 */
public interface RaftLogger {
    void debug(Object... v);

    void debugf(String format, Object... v);

    void error(Object... v);

    void errorf(String format, Object... v);

    void info(Object... v);

    void infof(String format, Object... v);

    void waring(Object... v);

    void waringf(String format, Object... v);

    void fatal(Object... v);

    void fatalf(String format, Object... v);

    void panic(Object... v);

    void panicf(String format, Object... v);


    int calldepth = Integer.MAX_VALUE;
    DefaultLogger raftLogger = new DefaultLogger();

    class DefaultLogger implements RaftLogger{
       boolean debug = false;
       final Logger logger = LoggerFactory.getLogger(DefaultLogger.class);
//        public static void main(String[] args) {
//            Logger logger = LoggerFactory.getLogger(DefaultLogger.class);
//        }

        public void enableDebug(){
            this.debug = true;
        }
        public void disableDebug(){
            this.debug = false;
        }
        @Override
        public void debug(Object... v) {
            if (debug){
                System.out.println(header("DEBUG",Arrays.deepToString(v)));
            }
        }

        @Override
        public void debugf(String format, Object... v) {
            if (debug){
                output(calldepth,header("DEBUG",String.format(format,v)));
            }
        }

        @Override
        public void error(Object... v) {

                output(calldepth,header("ERROR",Arrays.deepToString(v)));

        }

        @Override
        public void errorf(String format, Object... v) {
            output(calldepth, header("ERROR",String.format(format,v)));

        }

        @Override
        public void info(Object... v) {
            output(calldepth, header("INFO",Arrays.deepToString(v)));
        }

        @Override
        public void infof(String format, Object... v) {
            output(calldepth,  header("INFO",String.format(format,v)));
        }

        @Override
        public void waring(Object... v) {
            output(calldepth, header("WARN",Arrays.deepToString(v)));
        }

        @Override
        public void waringf(String format, Object... v) {
            output(calldepth, header("WARN",String.format(format, v)));
        }

        @Override
        public void fatal(Object... v) {
            output(calldepth, header("FATAL",Arrays.deepToString(v)));
            System.exit(1);
        }

        @Override
        public void fatalf(String format, Object... v) {
            output(calldepth, header("FATAL",String.format(format, v)));
            System.exit(1);
        }

        @Override
        public void panic(Object... v) {
            Util.panic(v);
        }

        @Override
        public void panicf(String format, Object... v) {
            Util.panic(format,v);
        }
        public String header(String lvl,String msg){
            return String.format("%s: %s",lvl,msg);
        }
        public void output(int calldepth,String message){
//            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            logger.debug("{} {}", "", message);
            System.out.format("{%s} {%s} \n", "", message);

        }
    }
}
