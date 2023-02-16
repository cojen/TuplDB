/*
 *  Copyright (C) 2023 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.ServerSocket;

import java.security.KeyStore;
import java.security.GeneralSecurityException;

import java.util.Base64;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteSecureTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteSecureTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        SSLContext context = createSSLContext();

        mServerDb = newTempDatabase(getClass());

        ServerSocket ss = context.getServerSocketFactory().createServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mClientDb = Database.connect(ss.getLocalSocketAddress(), context, 123456);
    }

    @After
    public void teardown() throws Exception {
        if (mClientDb != null) {
            mClientDb.close();
            mClientDb = null;
        }

        mServerDb = null;

        deleteTempDatabases(getClass());
    }

    private Database mServerDb;
    private Database mClientDb;

    @Test
    public void basic() throws Exception {
        Index ix = mClientDb.openIndex("test");

        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        ix.store(null, key, value);

        fastAssertArrayEquals(value, ix.load(null, key));
    }

    static SSLContext createSSLContext() throws GeneralSecurityException {
        SSLContext context = SSLContext.getInstance("TLSv1.2");

        KeyManagerFactory kmf = KeyManagerFactory.getInstance
            (KeyManagerFactory.getDefaultAlgorithm());

        KeyStore keystore = KeyStore.getInstance("PKCS12");
        char[] password = "password".toCharArray();

        byte[] keystoreBytes = Base64.getDecoder().decode(KEYSTORE);
        try {
            keystore.load(new ByteArrayInputStream(keystoreBytes), password);
        } catch (IOException e) {
            // Not expected.
        }

        kmf.init(keystore, password);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance
            (TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keystore);

        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return context;
    }

    /* 
       1. keytool -genkeypair -validity 36524 -keystore <filename> -keyalg RSA
       2. base64 <filename>
     */
    private static final String KEYSTORE = "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";
}
