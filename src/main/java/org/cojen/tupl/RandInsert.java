
package org.cojen.tupl;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.cojen.tupl.io.Utils;

public class RandInsert {
    public static void main(String[] args) throws Exception {
        var config = new DatabaseConfig().cacheSize(16_000_000_000L).directPageAccess(true);

        Database db = Database.open(config);

        var limit = new AtomicLong(1_000_000_000L); // number of records to insert
        int threadCount = 8;

        AtomicLong seeder = new AtomicLong(8675309);

        Runnable task = () -> {
            try {
                Index ix = db.openIndex("test");

                byte[] key = new byte[8];
                byte[] value = new byte[0];

                var rnd = new SplittableRandom(seeder.getAndAdd(43249823));

                var countdown = limit;
                long count = 0;

                while (true) {
                    rnd.nextBytes(key);
                    ix.store(Transaction.BOGUS, key, value);

                    if (++count % 1000 == 0) {
                        if (countdown.addAndGet(-1000) <= 0) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        };

        System.out.println("starting");

        long start = System.currentTimeMillis();
        var threads = new Thread[threadCount];

        for (int i=0; i<threadCount; i++) {
            (threads[i] = new Thread(task)).start();
        }

        for (Thread t : threads) {
            t.join();
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("duration: " + (duration / 1000.0) + " seconds");

        System.out.println(limit);
    }
}
