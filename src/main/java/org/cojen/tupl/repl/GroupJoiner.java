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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import java.nio.ByteBuffer;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import java.util.function.BiConsumer;

import java.util.logging.Level;

import org.cojen.tupl.io.Utils;

/**
 * Used to join a replication group by connecting to a set of seed members. At least one needs
 * to respond, as the current group leader, or provide the leader address instead.
 *
 * @author Brian S O'Neill
 */
class GroupJoiner {
    static final int OP_NOP = 0, OP_ERROR = 1, OP_ADDRESS = 2, OP_JOINED = 3;

    private final BiConsumer<Level, String> mEventListener;
    private final File mFile;
    private final long mGroupToken;
    private final SocketAddress mLocalAddress;
    private final SocketAddress mBindAddress;

    private Selector mSelector;
    private SocketChannel[] mSeedChannels;
    private SocketChannel mLeaderChannel;

    // Assigned if join succeeds.
    GroupFile mGroupFile;

    // Log index and term to start receiving from.
    long mPrevTerm;
    long mTerm;
    long mIndex;

    /**
     * @param groupFile file to store GroupFile contents
     * @param listenAddress optional
     */
    GroupJoiner(BiConsumer<Level, String> eventListener, File groupFile, long groupToken,
                SocketAddress localAddress, SocketAddress listenAddress)
    {
        mEventListener = eventListener;
        mFile = groupFile;
        mGroupToken = groupToken;
        mLocalAddress = localAddress;

        SocketAddress bindAddr = null;

        if (listenAddress instanceof InetSocketAddress) {
            bindAddr = new InetSocketAddress(((InetSocketAddress) listenAddress).getAddress(), 0);
        }

        mBindAddress = bindAddr;
    }

    /**
     * @throws IllegalStateException if already called
     */
    void join(Set<SocketAddress> seeds, int timeoutMillis) throws IOException {
        try {
            doJoin(seeds, timeoutMillis);
        } finally {
            close();
        }
    }

    private void doJoin(Set<SocketAddress> seeds, long timeoutMillis) throws IOException {
        if (seeds == null) {
            throw new IllegalArgumentException();
        }

        if (mSeedChannels != null) {
            throw new IllegalStateException();
        }

        // Asynchronously connect to all the seeds and wait for a response. All responses must
        // have a valid connect header. Non-leaders further respond with the leader address (if
        // available), and the leader responds with the term, index, and GroupFile.

        EncodingOutputStream out = new EncodingOutputStream();
        out.write(ChannelManager.newConnectHeader(mGroupToken, 0, 0, ChannelManager.TYPE_JOIN));
        out.write(OP_ADDRESS);
        out.encodeStr(mLocalAddress.toString());

        final byte[] command = out.toByteArray();

        mSelector = Selector.open();
        mSeedChannels = new SocketChannel[seeds.size()];

        int i = 0;
        for (SocketAddress addr : seeds) {
            SocketChannel channel = SocketChannel.open();
            mSeedChannels[i++] = channel;
            prepareChannel(channel, addr);
        }

        int expected = seeds.size();

        Set<String> joinFailureMessages = new TreeSet<>();
        Set<String> connectFailureMessages = new TreeSet<>();

        long end = System.currentTimeMillis() + timeoutMillis;

        while (expected > 0 && timeoutMillis > 0) {
            mSelector.select(timeoutMillis);

            Set<SelectionKey> keys = mSelector.selectedKeys();

            for (SelectionKey key : keys) {
                SocketChannel channel = (SocketChannel) key.channel();

                try {
                    if (key.isConnectable()) {
                        channel.finishConnect();
                        key.interestOps(SelectionKey.OP_WRITE);
                    } else if (key.isWritable()) {
                        // Assume the send buffer is large enough to fit the command.
                        channel.write(ByteBuffer.wrap(command));
                        key.interestOps(SelectionKey.OP_READ);
                    } else {
                        key.cancel();
                        channel.configureBlocking(true);
                        SocketAddress addr = processReply(channel.socket(), timeoutMillis);
                        expected--;

                        if (addr != null && mLeaderChannel == null && !seeds.contains(addr)) {
                            // Leader address was not one of the seeds, so connect now.
                            expected++;
                            channel = SocketChannel.open();
                            prepareChannel(channel, addr);
                            mLeaderChannel = channel;
                        }
                    }
                } catch (JoinException e) {
                    Utils.closeQuietly(channel);
                    expected--;
                    joinFailureMessages.add(e.getMessage());
                } catch (IOException e) {
                    Utils.closeQuietly(channel);
                    expected--;
                    connectFailureMessages.add(e.toString());
                }
            }

            keys.clear();

            if (mGroupFile != null) {
                return;
            }

            timeoutMillis = end - System.currentTimeMillis();
        }

        String fullMessage;

        Set<String> failureMessages = new LinkedHashSet<>();
        failureMessages.addAll(joinFailureMessages);
        failureMessages.addAll(connectFailureMessages);

        if (failureMessages.isEmpty()) {
            fullMessage = "timed out";
        } else {
            StringBuilder b = null;
            Iterator<String> it = failureMessages.iterator();

            while (true) {
                String message = it.next();
                if (b == null) {
                    if (!it.hasNext()) {
                        fullMessage = message;
                        break;
                    }
                    b = new StringBuilder();
                } else {
                    b.append("; ");
                }
                b.append(message);
                if (!it.hasNext()) {
                    fullMessage = b.toString();
                    break;
                }
            }
        }

        throw new JoinException(fullMessage);
    }

    private void prepareChannel(SocketChannel channel, SocketAddress addr) throws IOException {
        if (mBindAddress != null) {
            channel.bind(mBindAddress);
        }

        channel.configureBlocking(false);
        channel.register(mSelector, SelectionKey.OP_CONNECT);
        channel.connect(addr);
    }

    /**
     * @return leader address or null
     */
    private SocketAddress processReply(Socket s, long timeoutMillis) throws IOException {
        InputStream in = s.getInputStream();

        if (timeoutMillis >= 0) {
            int intTimeout;
            if (timeoutMillis == 0) {
                intTimeout = 1;
            } else {
                intTimeout = (int) Math.min(timeoutMillis, Integer.MAX_VALUE);
            }
            s.setSoTimeout(intTimeout);
        }

        SocketAddress addr = null;

        byte[] header = ChannelManager.readHeader(in, mGroupToken, 0);

        if (header != null) {
            ChannelInputStream cin = new ChannelInputStream(in, 1000);
            int op = cin.read();
            if (op == OP_ADDRESS) {
                addr = GroupFile.parseSocketAddress(cin.readStr(cin.readIntLE()));
                if (!(addr instanceof InetSocketAddress)
                    || ((InetSocketAddress) addr).getAddress().isAnyLocalAddress())
                {
                    // Invalid address.
                    addr = null;
                }
            } else if (op == OP_JOINED) {
                mPrevTerm = cin.readLongLE();
                mTerm = cin.readLongLE();
                mIndex = cin.readLongLE();
                try (FileOutputStream out = new FileOutputStream(mFile)) {
                    cin.drainTo(out);
                }
                mGroupFile = GroupFile.open(mEventListener, mFile, mLocalAddress, false);
            } else if (op == OP_ERROR) {
                throw new JoinException(ErrorCodes.toString(cin.readByte()));
            }
        }

        Utils.closeQuietly(s);

        return addr;
    }

    private void close() {
        Utils.closeQuietly(mSelector);
        mSelector = null;

        if (mSeedChannels != null) {
            for (SocketChannel channel : mSeedChannels) {
                Utils.closeQuietly(channel);
            }
            mSeedChannels = null;
        }

        Utils.closeQuietly(mLeaderChannel);
        mLeaderChannel = null;
    }
}
