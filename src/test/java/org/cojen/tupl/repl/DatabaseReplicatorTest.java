/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.File;

import java.net.ServerSocket;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.EventPrinter;
import org.cojen.tupl.Index;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.TestUtils;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DatabaseReplicatorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DatabaseReplicatorTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
        if (mDatabases != null) {
            for (Database db : mDatabases) {
                if (db != null) {
                    db.close();
                }
            }
        }

        TestUtils.deleteTempFiles(getClass());
    }

    private File[] mReplBaseFiles;
    private int[] mReplPorts;
    private ReplicatorConfig[] mReplConfigs;
    private DatabaseConfig[] mDbConfigs;
    private Database[] mDatabases;

    /**
     * @return first is the leader
     */
    private Database[] startGroup(int members) throws Exception {
        return startGroup(members, Role.NORMAL);
    }

    /**
     * @return first is the leader
     */
    private Database[] startGroup(int members, Role replicaRole) throws Exception {
        if (members < 1) {
            throw new IllegalArgumentException();
        }

        ServerSocket[] sockets = new ServerSocket[members];

        for (int i=0; i<members; i++) {
            sockets[i] = new ServerSocket(0);
        }

        mReplBaseFiles = new File[members];
        mReplPorts = new int[members];
        mReplConfigs = new ReplicatorConfig[members];
        mDbConfigs = new DatabaseConfig[members];
        mDatabases = new Database[members];

        for (int i=0; i<members; i++) {
            mReplBaseFiles[i] = TestUtils.newTempBaseFile(getClass()); 
            mReplPorts[i] = sockets[i].getLocalPort();

            mReplConfigs[i] = new ReplicatorConfig()
                .groupToken(1)
                .localSocket(sockets[i]);

            if (i > 0) {
                mReplConfigs[i].addSeed(sockets[0].getLocalSocketAddress());
                mReplConfigs[i].localRole(replicaRole);
            }

            mDbConfigs[i] = new DatabaseConfig()
                .baseFile(mReplBaseFiles[i])
                .replicate(mReplConfigs[i])
                //.eventListener(new EventPrinter())
                .directPageAccess(false);

            Database db = Database.open(mDbConfigs[i]);
            mDatabases[i] = db;

            readyCheck: {
                for (int trial=0; trial<100; trial++) {
                    Thread.sleep(100);

                    if (i == 0) {
                        try {
                            db.openIndex("first");
                            // Ensure that replicas obtain the index in the snapshot.
                            db.checkpoint();
                            break readyCheck;
                        } catch (UnmodifiableReplicaException e) {
                            // Not leader yet.
                        }
                    } else {
                        assertNotNull(db.openIndex("first"));
                        break readyCheck;
                    }
                }

                throw new AssertionError(i == 0 ? "No leader" : "Not joined");
            }
        }

        return mDatabases;
    }

    @Test
    public void basicTestOneMember() throws Exception {
        basicTest(1);
    }

    @Test
    public void basicTestThreeMembers() throws Exception {
        for (int i=3; --i>=0; ) {
            try {
                basicTest(3);
                break;
            } catch (UnmodifiableReplicaException e) {
                // Test is load sensitive and leadership is sometimes lost.
                // https://github.com/cojen/Tupl/issues/70
                if (i <= 0) {
                    throw e;
                }
            }
        }
    }

    private void basicTest(int memberCount) throws Exception {
        Database[] dbs = startGroup(memberCount);

        Index ix0 = dbs[0].openIndex("test");

        for (int t=0; t<10; t++) {

            byte[] key = ("hello-" + t).getBytes();
            byte[] value = ("world-" + t).getBytes();
            ix0.store(null, key, value);

            TestUtils.fastAssertArrayEquals(value, ix0.load(null, key));

            for (int i=0; i<dbs.length; i++) {
                for (int q=100; --q>=0; ) {
                    byte[] actual = null;

                    try {
                        Index ix = dbs[i].openIndex("test");
                        actual = ix.load(null, key);
                        TestUtils.fastAssertArrayEquals(value, actual);
                        break;
                    } catch (UnmodifiableReplicaException e) {
                        // Index doesn't exist yet due to replication delay.
                        if (q == 0) {
                            throw e;
                        }
                    } catch (AssertionError e) {
                        // Value doesn't exist yet due to replication delay.
                        if (q == 0 || actual != null) {
                            throw e;
                        }
                    }

                    TestUtils.sleep(100);
                }
            }
        }
    }
}
