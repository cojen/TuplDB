/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl.core;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.reflect.Constructor;

import java.util.HashMap;
import java.util.Map;

import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.CRC32C;

import org.cojen.tupl.Database;
import org.cojen.tupl.Snapshot;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.repl.SnapshotReceiver;
import org.cojen.tupl.repl.SnapshotSender;
import org.cojen.tupl.repl.StreamReplicator;

/**
 * Replication utility methods.
 *
 * @author Brian S O'Neill
 */
final class ReplUtils extends Utils {
    private static final long RESTORE_EVENT_RATE_MILLIS = 5000;

    /**
     * Called when the database is created, in an attempt to retrieve an existing database
     * snapshot from a replication peer. If null is returned, the database will try to start
     * reading replication data at the lowest position.
     *
     * @param listener optional restore event listener
     * @return null if no snapshot could be found
     * @throws IOException if a snapshot was found, but requesting it failed
     */
    public static InputStream restoreRequest(StreamReplicator repl, EventListener listener)
        throws IOException
    {
        Map<String, String> options = new HashMap<>();
        options.put("checksum", "CRC32C");

        Constructor<?> lz4Input;
        try {
            Class<?> clazz = Class.forName("net.jpountz.lz4.LZ4FrameInputStream");
            lz4Input = clazz.getConstructor(InputStream.class);
            options.put("compress", "LZ4Frame");
        } catch (Throwable e) {
            lz4Input = null;
        }

        SnapshotReceiver receiver = repl.restore(options);

        if (receiver == null) {
            return null;
        }

        InputStream in;

        try {
            in = receiver.inputStream();
            options = receiver.options();
            long length = receiver.length();

            String compressOption = options.get("compress");

            if (compressOption != null) {
                if (compressOption.equals("LZ4Frame")) {
                    try {
                        in = (InputStream) lz4Input.newInstance(in);
                    } catch (Throwable e) {
                        throw new IOException("Unable to decompress", e);
                    }
                } else {
                    throw new IOException("Unknown compress option: " + compressOption);
                }
            }

            String checksumOption = options.get("checksum");

            if (checksumOption != null) {
                if (checksumOption.equals("CRC32C")) {
                    in = new CheckedInputStream(in, new CRC32C(), length);
                } else {
                    throw new IOException("Unknown checksum option: " + checksumOption);
                }
            }

            if (listener != null && length >= 0) {
                var rin = new RestoreInputStream(in);
                in = rin;

                listener.notify(EventType.REPLICATION_RESTORE,
                                "Receiving snapshot: %1$,d bytes from %2$s",
                                length, receiver.senderAddress());

                new Progress(listener, rin, length).start();
            }
        } catch (Throwable e) {
            closeQuietly(receiver);
            throw e;
        }

        return in;
    }

    private static final class Progress extends Thread {
        private final EventListener mListener;
        private final RestoreInputStream mRestore;
        private final long mLength;

        private long mLastTimeMillis = Long.MIN_VALUE;
        private long mLastReceived;

        Progress(EventListener listener, RestoreInputStream in, long length) {
            mListener = listener;
            mRestore = in;
            mLength = length;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (!mRestore.isFinished()) {
                long now = System.currentTimeMillis();

                long received = mRestore.received();
                double percent = 100.0 * (received / (double) mLength);
                long progress = received - mLastReceived;

                if (mLastTimeMillis != Long.MIN_VALUE) {
                    double rate = (1000.0 * (progress / (double) (now - mLastTimeMillis)));
                    String format = "Receiving snapshot: %1$1.3f%%";
                    if (rate == 0) {
                        mListener.notify(EventType.REPLICATION_RESTORE, format, percent);
                    } else {
                        format += "  rate: %2$,d bytes/s  remaining: ~%3$s";
                        long remainingSeconds = (long) ((mLength - received) / rate);
                        mListener.notify
                            (EventType.REPLICATION_RESTORE, format,
                             percent, (long) rate, remainingDuration(remainingSeconds));
                    }
                }

                mLastTimeMillis = now;
                mLastReceived = received;

                try {
                    Thread.sleep(RESTORE_EVENT_RATE_MILLIS);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        private static String remainingDuration(long seconds) {
            if (seconds < (60 * 2)) {
                return seconds + "s";
            } else if (seconds < (60 * 60 * 2)) {
                return (seconds / 60) + "m";
            } else if (seconds < (60 * 60 * 24 * 2)) {
                return (seconds / (60 * 60)) + "h";
            } else {
                return (seconds / (60 * 60 * 24)) + "d";
            }
        }
    }

    public static void sendSnapshot(Database db, SnapshotSender sender) throws IOException {
        Map<String, String> requestedOptions = sender.options();

        var options = new HashMap<String, String>();

        Checksum checksum = null;
        if ("CRC32C".equals(requestedOptions.get("checksum"))) {
            options.put("checksum", "CRC32C");
            checksum = new CRC32C();
        }

        Snapshot snapshot = db.beginSnapshot();

        Constructor lz4Output = null;
        if (snapshot.isCompressible() && "LZ4Frame".equals(requestedOptions.get("compress"))) {
            try {
                Class<?> clazz = Class.forName("net.jpountz.lz4.LZ4FrameOutputStream");
                lz4Output = clazz.getConstructor(OutputStream.class);
                options.put("compress", "LZ4Frame");
            } catch (Throwable e) {
                // Not supported.
            }
        }

        OutputStream out = sender.begin(snapshot.length(), snapshot.position(), options);
        try {
            if (lz4Output != null) {
                try {
                    out = (OutputStream) lz4Output.newInstance(out);
                } catch (Throwable e) {
                    throw new IOException("Unable to compress", e);
                }
            }

            CheckedOutputStream cout = null;
            if (checksum != null) {
                out = cout = new CheckedOutputStream(out, checksum);
            }

            snapshot.writeTo(out);

            if (cout != null) {
                var buf = new byte[4];
                encodeIntLE(buf, 0, (int) checksum.getValue());
                out.write(buf);
            }
        } finally {
            out.close();
        }
    }
}
