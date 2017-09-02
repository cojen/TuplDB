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
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import java.nio.ByteBuffer;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import java.util.Set;

import org.cojen.tupl.io.Utils;

/**
 * Used to join a replication group by connecting to a set of seed members. At least one needs
 * to respond, as the current group leader, or provide the leader address instead.
 *
 * @author Brian S O'Neill
 */
class GroupJoiner {
    // FIXME: testing
    public static void main(String[] args) throws Exception {
        GroupJoiner joiner = new GroupJoiner
            (new File("foo"), 1, new java.net.InetSocketAddress("localhost", 1234));

        Set<SocketAddress> seeds = new java.util.HashSet<>();
        for (int i=0; i<args.length; i+=2) {
            seeds.add(new java.net.InetSocketAddress(args[i], Integer.parseInt(args[i + 1])));
        }

        joiner.join(seeds, 0);

        joiner.close();
    }

    static final int OP_NOP = 0, OP_ERROR = 1, OP_ADDRESS = 2, OP_JOINED = 3;

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
     */
    GroupJoiner(File groupFile, long groupToken, SocketAddress localAddress) {
        mFile = groupFile;
        mGroupToken = groupToken;
        mLocalAddress = localAddress;

        SocketAddress bindAddr = null;

        if (localAddress instanceof InetSocketAddress) {
            bindAddr = new InetSocketAddress(((InetSocketAddress) localAddress).getAddress(), 0);
        }

        mBindAddress = bindAddr;
    }

    /**
     * @throws IllegalStateException if already called
     */
    boolean join(Set<SocketAddress> seeds, int timeoutMillis) throws IOException {
        try {
            return doJoin(seeds, timeoutMillis);
        } finally {
            close();
        }
    }

    private boolean doJoin(Set<SocketAddress> seeds, long timeoutMillis) throws IOException {
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
                        SocketAddress addr = processReply(channel.socket());
                        expected--;

                        if (addr != null && mLeaderChannel == null && !seeds.contains(addr)) {
                            // Leader address was not one of the seeds, so connect now.
                            expected++;
                            channel = SocketChannel.open();
                            prepareChannel(channel, addr);
                            mLeaderChannel = channel;
                        }
                    }
                } catch (IOException e) {
                    Utils.closeQuietly(e, channel);
                    expected--;
                }
            }

            keys.clear();

            if (mGroupFile != null) {
                return true;
            }

            timeoutMillis = end - System.currentTimeMillis();
        }

        return false;
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
    private SocketAddress processReply(Socket s) throws IOException {
        SocketAddress addr = null;

        byte[] header = ChannelManager.readHeader(s, mGroupToken, 0);

        if (header != null) {
            ChannelInputStream in = new ChannelInputStream(s.getInputStream(), 1000);
            int op = in.read();
            if (op == OP_ADDRESS) {
                addr = GroupFile.parseSocketAddress(in.readStr(in.readIntLE()));
                if (!(addr instanceof InetSocketAddress)
                    || ((InetSocketAddress) addr).getAddress().isAnyLocalAddress())
                {
                    // Invalid address.
                    addr = null;
                }
            } else if (op == OP_JOINED) {
                mPrevTerm = in.readLongLE();
                mTerm = in.readLongLE();
                mIndex = in.readLongLE();
                try (FileOutputStream out = new FileOutputStream(mFile)) {
                    in.drainTo(out);
                }
                mGroupFile = GroupFile.open(mFile, mLocalAddress, false);
            }
        }

        Utils.closeQuietly(null, s);

        return addr;
    }

    private void close() {
        Utils.closeQuietly(null, mSelector);
        mSelector = null;

        if (mSeedChannels != null) {
            for (SocketChannel channel : mSeedChannels) {
                Utils.closeQuietly(null, channel);
            }
            mSeedChannels = null;
        }

        Utils.closeQuietly(null, mLeaderChannel);
        mLeaderChannel = null;
    }
}
