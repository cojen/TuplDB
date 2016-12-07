/*
 *  Copyright 2012-2015 Cojen.org
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
            AAA_PageAccessTransformerTest.class,
            CommitLockTest.class,
            ConfigTest.class,
            CrudTest.class,
            CrudDirectTest.class,
            CrudDirectMappedTest.class,
            CrudNonDurableTest.class,
            CrudNonTransformTest.class,
            CrudBasicTransformTest.class,
            CrudBasicFilterTest.class,
            CrudDefaultTest.class,
            CursorTest.class,
            CursorDirectTest.class,
            CursorDirectMappedTest.class,
            CursorNonDurableTest.class,
            CursorNonTransformTest.class,
            CursorBasicTransformTest.class,
            CursorBasicFilterTest.class,
            CursorDefaultTest.class,
            CountTest.class,
            LargeKeyTest.class,
            LargeKeyDirectTest.class,
            LargeValueTest.class,
            LargeValueDirectTest.class,
            LargeValueDirectMappedTest.class,
            LargeValueNonDurableTest.class,
            LargeValueChaosTest.class,
            ExtraLargeValueTest.class,
            ExtraLargeValueDirectTest.class,
            ExtraLargeValueNonDurableTest.class,
            FileIOTest.class,
            LockTest.class,
            DeadlockTest.class,
            RecoverTest.class,
            RecoverDirectTest.class,
            RecoverMappedTest.class,
            RecoverMappedDirectTest.class,
            SnapshotTest.class,
            SnapshotDirectTest.class,
            SnapshotCryptoTest.class,
            SnapshotCryptoDirectTest.class,
            SnapshotMappedTest.class,
            SnapshotMappedDirectTest.class,
            TransactionTest.class,
            TransactionDirectTest.class,
            TransactionNonDurableTest.class,
            UtilsTest.class,
            SmallCacheTest.class,
            ViewTest.class,
            ViewDirectTest.class,
            CloseTest.class,
            CloseDirectTest.class,
            CloseDirectMappedTest.class,
            CloseNonDurableTest.class,
            RenameTest.class,
            RenameDirectTest.class,
            //StreamTest.class,
            //StreamDirectTest.class,
            PageSizeTest.class,
            PageSizeDirectTest.class,
            CompactTest.class,
            CompactDirectTest.class,
            NodeMapTest.class,
            PageCacheTest.class,
            CustomLogTest.class,
            LimitCapacityTest.class,
            TransformerTest.class,
            AnalyzeTest.class,
            AnalyzeDirectTest.class,
            EvictionTest.class,
            EvictionDirectTest.class,
            EnduranceTest.class,
            EnduranceDirectTest.class,
            ContentionTest.class,
            ContentionDirectTest.class,
            ContendedLockTest.class,
            DirectPageOpsTest.class,
            UnreplicatedTest.class,
            TempIndexTest.class,
        };

        String[] names = new String[classes.length];
        for (int i=0; i<classes.length; i++) {
            names[i] = classes[i].getName();
        }

        org.junit.runner.JUnitCore.main(names);
    }
}
