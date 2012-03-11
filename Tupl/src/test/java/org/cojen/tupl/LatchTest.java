/*
 *  Copyright 2011 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class LatchTest {
    static int count;

    public static void main(String[] args) throws Exception {
        final Latch latch = new Latch();

        int readers = Integer.parseInt(args[0]);
        int writers = Integer.parseInt(args[1]);

        for (int i=0; i<readers; i++) {
            new Thread() {
                public void run() {
                    while (true) {
                        latch.acquireShared();
                        boolean upgraded = false;
                        try {
                            System.out.println("read latch:  " + this + ", " + count);
                            if (upgraded = latch.tryUpgrade()) {
                                ++count;
                                System.out.println("upgraded:   " + this + ", " + count);
                            }
                        } finally {
                            latch.release(upgraded);
                        }
                    }
                }
            }.start();
        }

        for (int i=0; i<writers; i++) {
            new Thread() {
                public void run() {
                    while (true) {
                        latch.acquireExclusive();
                        boolean downgraded = false;
                        try {
                            ++count;
                            System.out.println("write latch: " + this + ", " + count);
                            if (count % 10 == 0) {
                                latch.downgrade();
                                downgraded = true;
                            }
                        } finally {
                            latch.release(!downgraded);
                        }
                    }
                }
            }.start();
        }

    }
}
