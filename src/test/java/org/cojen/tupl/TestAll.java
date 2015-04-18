/*
 *  Copyright 2012-2013 Brian S O'Neill
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
@org.junit.Ignore
public class TestAll {
    public static void main(String[] args) throws Exception {
        Class[] classes = {
            ConfigTest.class,
            CrudTest.class,
            CrudNonDurableTest.class,
            CrudNonTransformTest.class,
            CrudBasicTransformTest.class,
            CrudBasicFilterTest.class,
            CursorTest.class,
            CursorNonDurableTest.class,
            CursorNonTransformTest.class,
            CursorBasicTransformTest.class,
            CursorBasicFilterTest.class,
            LargeKeyTest.class,
            LargeValueTest.class,
            LargeValueNonDurableTest.class,
            ExtraLargeValueTest.class,
            ExtraLargeValueNonDurableTest.class,
            LockTest.class,
            DeadlockTest.class,
            RecoverTest.class,
            RecoverMappedTest.class,
            SnapshotTest.class,
            SnapshotCryptoTest.class,
            SnapshotMappedTest.class,
            TransactionTest.class,
            TransactionNonDurableTest.class,
            UtilsTest.class,
            SmallCacheTest.class,
            ViewTest.class,
            CloseTest.class,
            CloseNonDurableTest.class,
            RenameTest.class,
            StreamTest.class,
            PageSizeTest.class,
            CompactTest.class,
            NodeMapTest.class,
            PageCacheTest.class,
            CustomLogTest.class,
        };

        String[] names = new String[classes.length];
        for (int i=0; i<classes.length; i++) {
            names[i] = classes[i].getName();
        }

        org.junit.runner.JUnitCore.main(names);
    }
}
