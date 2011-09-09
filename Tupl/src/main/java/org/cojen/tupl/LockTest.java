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
public class LockTest {
    public static void main(String[] args) throws Exception {
        LockManager m = new LockManager();
        Locker locker = new Locker(m);
        byte[] k1 = "hello".getBytes();
        byte[] k2 = "world".getBytes();
        System.out.println(locker.lockShared(k1, 100));
        System.out.println(locker.lockShared(k1, 100));
        System.out.println(locker.lockShared(k1, 100));
        locker.unlock();
        try {
            locker.unlock();
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println(locker.lockShared(k1, 100));
        locker.unlock();
        System.out.println(locker.lockShared(k1, 100));
        System.out.println(locker.lockShared(k1, 100));
        locker.unlock();
        System.out.println(locker.lockShared(k2, 100));
        try {
            locker.unlock(k1);
        } catch (Exception e) {
            System.out.println(e);
        }
        locker.unlock(k2);
        System.out.println(locker.lockShared(k2, 100));
        System.out.println(locker.lockExclusive(k2, 100));
        System.out.println(locker.lockShared(k2, 100));
        locker.unlock(k2);
        System.out.println(locker.lockExclusive(k2, 100));
        System.out.println(locker.lockShared(k2, 100));
        System.out.println(locker.lockUpgradable(k2, 100));
        try {
            locker.unlockToShared(k1);
        } catch (Exception e) {
            System.out.println(e);
        }
        locker.unlockToShared(k2);
        System.out.println(locker.lockShared(k2, 100));

        System.out.println("---");
        Locker locker2 = new Locker(m);
        System.out.println(locker2.lockUpgradable(k2, 100));
        System.out.println(locker2.lockExclusive(k2, 1000000000L));
        locker.unlock(k2);
        System.out.println(locker2.lockExclusive(k2, 1000000000L));
    }
}
