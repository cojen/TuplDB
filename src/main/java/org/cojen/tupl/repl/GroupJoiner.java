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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import java.util.function.Consumer;

import org.cojen.tupl.diag.EventListener;

import org.cojen.tupl.io.Utils;

/**
 * Used to join/unjoin a replication group by connecting to a set of seed members. At least one
 * needs to respond, as the current group leader, or provide the leader address instead.
 *
 * @author Brian S O'Neill
 */
class GroupJoiner {
    static final int OP_NOP = 0, OP_ERROR = 1, OP_ADDRESS = 2, OP_JOINED = 3,
        OP_UNJOIN_ADDRESS = 4, OP_UNJOIN_MEMBER = 5, OP_UNJOINED = 6;

    private final EventListener mEventListener;
    private final File mFile;
    private final long mGroupToken1, mGroupToken2;
    private final SocketAddress mLocalAddress;
    private final SocketAddress mBindAddress;

    private Selector mSelector;
    private SocketChannel[] mSeedChannels;
    private SocketChannel mLeaderChannel;

    // Assigned if join succeeds.
    GroupFile mGroupFile;
    boolean mReplySuccess;

    // Log position and term to start receiving from.
    long mPrevTerm;
    long mTerm;
    long mPosition;

    /**
     * @param groupFile file to store GroupFile contents
     * @param listenAddress optional
     */
    GroupJoiner(EventListener eventListener, File groupFile, long groupToken1, long groupToken2,
                SocketAddress localAddress, SocketAddress listenAddress)
    {
        mEventListener = eventListener;
        mFile = groupFile;
        mGroupToken1 = groupToken1;
        mGroupToken2 = groupToken2;
        mLocalAddress = localAddress;

        SocketAddress bindAddr = null;

        if (listenAddress instanceof InetSocketAddress isa) {
            bindAddr = new InetSocketAddress(isa.getAddress(), 0);
        }

        mBindAddress = bindAddr;
    }

    /**
     * Constructor which can be used to unjoin.
     */
    GroupJoiner(long groupToken1, long groupToken2) {
        this(null, null, groupToken1, groupToken2, null, null);
    }

    /**
     * @throws IllegalStateException if already called
     */
    void join(Set<SocketAddress> seeds, int timeoutMillis) throws IOException {
        try {
            doJoin(seeds, timeoutMillis, out -> {
                out.write(OP_ADDRESS);
                out.encodeStr(GroupFile.addressToString(mLocalAddress));
            });
        } finally {
            close();
        }
    }

    /**
     * @throws IllegalStateException if already called
     */
    void unjoin(Set<SocketAddress> seeds, int timeoutMillis, long memberId) throws IOException {
        try {
            doJoin(seeds, timeoutMillis, out -> {
                out.write(OP_UNJOIN_MEMBER);
                out.encodeLongLE(memberId);
            });
        } finally {
            close();
        }
    }

    /**
     * @throws IllegalStateException if already called
     */
    void unjoin(Set<SocketAddress> seeds, int timeoutMillis, SocketAddress memberAddr)
        throws IOException
    {
        try {
            doJoin(seeds, timeoutMillis, out -> {
                out.write(OP_UNJOIN_ADDRESS);
                out.encodeStr(GroupFile.addressToString(memberAddr));
            });
        } finally {
            close();
        }
    }

    private void doJoin(Set<SocketAddress> seeds, long timeoutMillis,
                        Consumer<EncodingOutputStream> cout)
        throws IOException
    {
        if (seeds == null) {
            throw new IllegalArgumentException();
        }

        if (mSeedChannels != null) {
            throw new IllegalStateException();
        }

        // Asynchronously connect to all the seeds and wait for a response. All responses must
        // have a valid connect header. Non-leaders further respond with the leader address (if
        // available), and the leader responds with the term, position, and GroupFile.

        var out = new EncodingOutputStream();
        out.write(ChannelManager.newConnectHeader(0, 0, ChannelManager.TYPE_JOIN,
                                                  mGroupToken1, mGroupToken2));

        cout.accept(out);

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

        var joinFailureMessages = new TreeSet<String>();
        var connectFailureMessages = new TreeSet<String>();

        long end = System.currentTimeMillis() + timeoutMillis;

        while (expected > 0 && timeoutMillis > 0) {
            mSelector.select(timeoutMillis);

            Set<SelectionKey> keys = mSelector.selectedKeys();

            for (SelectionKey key : keys) {
                var channel = (SocketChannel) key.channel();

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

            if (mReplySuccess) {
                return;
            }

            timeoutMillis = end - System.currentTimeMillis();
        }

        String fullMessage;

        var failureMessages = new LinkedHashSet<String>();
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

        byte[] header = ChannelManager.readHeader(s, false, 0, mGroupToken1, mGroupToken2);

        if (header != null) {
            var cin = new ChannelInputStream(s.getInputStream(), 1000, false);
            int op = cin.read();
            if (op == OP_ADDRESS) {
                addr = GroupFile.parseSocketAddress(cin.readStr(cin.readIntLE()));
                if (!(addr instanceof InetSocketAddress isa)
                    || isa.getAddress().isAnyLocalAddress())
                {
                    // Invalid address.
                    addr = null;
                }
            } else if (op == OP_JOINED) {
                mPrevTerm = cin.readLongLE();
                mTerm = cin.readLongLE();
                mPosition = cin.readLongLE();
                try (var out = new FileOutputStream(mFile)) {
                    cin.drainTo(out);
                }
                mGroupFile = GroupFile.open(mEventListener, mFile, mLocalAddress, false);
                mReplySuccess = true;
            } else if (op == OP_UNJOINED) {
                mReplySuccess = true;
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
