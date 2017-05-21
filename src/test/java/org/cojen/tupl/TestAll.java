/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
            CrudSelfUnionTest.class,
            CrudSelfIntersectionTest.class,
            CrudSelfDifferenceTest.class,
            CrudDisjointUnionTest.class,
            CursorTest.class,
            CursorDirectTest.class,
            CursorDirectMappedTest.class,
            CursorNonDurableTest.class,
            CursorNonTransformTest.class,
            CursorBasicTransformTest.class,
            CursorBasicFilterTest.class,
            CursorDefaultTest.class,
            CursorDisjointUnionTest.class,
            CountTest.class,
            LargeKeyTest.class,
            LargeKeyDirectTest.class,
            LargeValueTest.class,
            LargeValueDirectTest.class,
            LargeValueDirectMappedTest.class,
            LargeValueNonDurableTest.class,
            LargeValueFuzzTest.class,
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
            ReplicationTest.class,
            TempIndexTest.class,
            WorkerTest.class,
            MergeViewTest.class,
            UnionViewTest.class,
            ScannerTest.class,
            UpdaterTest.class,
        };

        String[] names = new String[classes.length];
        for (int i=0; i<classes.length; i++) {
            names[i] = classes[i].getName();
        }

        org.junit.runner.JUnitCore.main(names);
    }
}
