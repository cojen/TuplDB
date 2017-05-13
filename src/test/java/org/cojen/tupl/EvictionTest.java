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

import static org.cojen.tupl.TestUtils.deleteTempDatabases;
import static org.cojen.tupl.TestUtils.newTempDatabase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class EvictionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(EvictionTest.class.getName());
    }
    
    @Parameters(name="{index}: size[{0}], autoLoad[{1}]")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { 1024, false }, // single item fits on a page, autoLoad is false
            { 1024, true }, // single item fits on a page, autoLoad is true
            { 256, false }, // at least two items on a page, autoLoad is false
            { 256, true }, // at least two items on a page, autoLoad is true
            { 128, false }, // more than two items on a page, autoLoad is false
            { 128, true },  // more than two items on a page, autoLoad is true
        });
    }
    

    public EvictionTest(int size, boolean autoLoad) {
        mSize = size;
        mAutoLoad = autoLoad;
    }

    private int mSize;
    private boolean mAutoLoad;
    protected Database mDb;
    
    static class TestEvictionFilter implements Filter {
        
        public final ArrayList<byte[]> mKeys;
        public final ArrayList<byte[]> mValues;
        
        public TestEvictionFilter() {
            mKeys = new ArrayList<>();
            mValues = new ArrayList<>();
        }
        
        @Override
        public boolean isAllowed(byte[] key, byte[] value) throws IOException {
            mKeys.add(key);
            mValues.add(value);
            return true;
        }
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase(getClass(),
                              new DatabaseConfig().pageSize(2048)
                              .minCacheSize(1_000_000)
                              .maxCacheSize(1_000_000)    // cacheSize ~ 500 nodes
                              .durabilityMode(DurabilityMode.NO_FLUSH)
                              .directPageAccess(false));
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }
    
    @Test
    public void testEvictBase() throws IOException {
        int size = mSize;
        boolean autoLoad = mAutoLoad;
        Index ix = mDb.openIndex("test");
        int initialRecordCount = 100_000 * (1024/size);
        for (int i=0; i<initialRecordCount; i++) { 
            String key = textOfLength(i, 'k', size);
            String val = textOfLength(i, 'v', size);
            ix.store(null, key.getBytes(), val.getBytes());
        }
        
        // basic eviction
        TestEvictionFilter evictionFilter = new TestEvictionFilter();
        long evicted = ix.evict(null, null, null, evictionFilter, autoLoad);
        int recordCount = initialRecordCount - evictionFilter.mKeys.size();
        long keyValueSize = 0; 
        for (int i = 0; i < evictionFilter.mKeys.size(); ++i) {
            keyValueSize += evictionFilter.mKeys.get(i).length + evictionFilter.mValues.get(i).length; 
        }
        assertEquals(evicted, autoLoad ? keyValueSize : keyValueSize * 2); //if autoload is not enabled, only keys are load 
        assertEquals(recordCount, ix.count(null, null));
        for (byte[] key: evictionFilter.mKeys) {
            assertNull(ix.load(null, key));
        }
        assertTrue(evictionFilter.mKeys.size() >= 1);
        assertTrue(evictionFilter.mKeys.size() <= 1024/size);
        
        // empty range
        evictionFilter = new TestEvictionFilter();
        assertEquals(0, ix.evict(null, "a".getBytes(), "b".getBytes(), evictionFilter, autoLoad));
        assertEquals(recordCount, ix.count(null, null));
        assertEquals(0, evictionFilter.mKeys.size());
        assertEquals(0, evictionFilter.mValues.size());
        
        // evict nodes in a particular range
        evictionFilter = new TestEvictionFilter();
        ix.newCursor(null).find("a".getBytes());    // loads rightmost nodes at all levels into cache
        assertEquals(size * 2, ix.evict(null, "009998".getBytes(), "009999".getBytes(), evictionFilter, autoLoad));
        assertEquals(1, evictionFilter.mKeys.size());
        assertEquals(1, evictionFilter.mValues.size());
        assertTrue(new String(evictionFilter.mKeys.get(0)).startsWith("009998"));
        if (autoLoad) {
            assertTrue(new String(evictionFilter.mValues.get(0)).startsWith("009998"));
        } else {
            assertTrue(evictionFilter.mValues.get(0) == Cursor.NOT_LOADED);
        }
        assertEquals(--recordCount, ix.count(null, null));
        
        // ghost records
        evictionFilter = new TestEvictionFilter();
        Transaction txn = mDb.newTransaction();
        int lowKey = initialRecordCount-11;
        int highKey = initialRecordCount-20;
        for (int i=lowKey; i<=highKey; i++) {
            String key = textOfLength(i, 'k', size);
            ix.delete(txn, key.getBytes());
        }
        assertEquals(0, ix.evict(null, String.valueOf(lowKey).getBytes(), String.valueOf(highKey).getBytes(), evictionFilter, autoLoad));
        assertEquals(0, evictionFilter.mKeys.size());
        assertEquals(0, evictionFilter.mValues.size());
        txn.reset();
        
        VerificationObserver observer = new VerificationObserver();
        ix.verify(observer);
        assertFalse(observer.failed);
    }


    private String textOfLength(int prefix, char c, int len) {
        len-=6;
        char[] chars = new char[len];
        Arrays.fill(chars, c);
        return String.format("%06d%s", prefix, new String(chars));
    }
}
