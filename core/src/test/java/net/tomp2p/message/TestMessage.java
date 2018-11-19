/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.Random;

import net.tomp2p.p2p.PeerAddressManager;
import net.tomp2p.peers.*;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Triple;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import net.tomp2p.Utils2;
import org.whispersystems.curve25519.Curve25519KeyPair;

/**
 * Tests encoding of an empty message. These tests should not be used for
 * performance measuremenst, since mockito is used, and I guess this would
 * falsify the results.
 * <p>
 * As a reference, on my laptop IBM T60 its around 190 enc-dec/ms, so we can
 * encode and decode with a single core 192'000 messages per second. For a
 * message smallest size with 59bytes, this means that we can saturate an
 * 86mbit/s link. The larger the message, the more bandwidth it is used
 * (QuadCore~330 enc-dec/ms).
 *
 * @author Thomas Bocek
 */
public class TestMessage {

    private static final int SEED = 1;
    private static final int BIT_16 = 256 * 256;
    private static final Random RND = new Random(SEED);

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };


    /**
     * Test a simple message to endcode and then decode.
     *
     * @throws Exception .
     */
    @Test
    public void testEncodeDecode() throws Exception {
        Triple<Message, Curve25519KeyPair, Curve25519KeyPair> p1 = Utils2.createDummyMessage();
        final int size = 100;
        Random rnd = new Random(42);
        byte[] b = new byte[size];
        rnd.nextBytes(b);

        p1.element0().payload(ByteBuffer.wrap(b));
        Message m2 = encodeDecode(p1.element0(), p1.element1().getPrivateKey(), p1.element2().getPrivateKey(), size + Codec.HEADER_SIZE_MIN);
        compareMessage(p1.element0(), m2);
        Assert.assertTrue(m2.isDone());
    }

    @Test
    public void testBigData() throws Exception {
        Triple<Message, Curve25519KeyPair, Curve25519KeyPair> p1 = Utils2.createDummyMessage();
        final int size = 50 * 1000 * 1000;
        Random rnd = new Random(42);
        byte[] b = new byte[size];
        rnd.nextBytes(b);

        p1.element0().payload(ByteBuffer.wrap(b));
        Message m2 = encodeDecode(p1.element0(), p1.element1().getPrivateKey(), p1.element2().getPrivateKey(), size + Codec.HEADER_SIZE_MIN);
        compareMessage(p1.element0(), m2);
        Assert.assertTrue(m2.isDone());
    }

    private Message encodeDecode(final Message m1, final byte[] privateKeySender, final byte[] privateKeyRecipient, int bufferSize)
            throws GeneralSecurityException, IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferSize]);
        Codec.encode(buffer, m1, create(m1.sender(), privateKeySender), null,true);
        Message m2 = new Message();
        buffer.flip();
        MessageHeader messageHeader = Codec.decodeHeader(buffer, create(m1.recipient(), privateKeyRecipient));
        Codec.decodePayload(buffer, m2, messageHeader, null, m1.recipient().createSocket(m1.sender()), m1.sender().createSocket(m1.recipient()));
        return m2;
    }

    PeerAddressManager create(final PeerAddress peerAddress, final byte[] privateKey) {
        return new PeerAddressManager() {
            @Override
            public Pair<PeerAddress, byte[]> getPeerAddressFromShortId(int recipientShortId) {
                return Pair.of(peerAddress, privateKey);
            }

            @Override
            public Pair<PeerAddress, byte[]> getPeerAddressFromId(Number256 peerId) {
                return getPeerAddressFromShortId(0);
            }
        };
    }


    /**
     * Checks if two messages are the same.
     *
     * @param m1 The first message
     * @param m2 The second message
     */
    private void compareMessage(final Message m1, final Message m2) {
        Assert.assertEquals(m1.messageId(), m2.messageId());
        Assert.assertEquals(m1.version(), m2.version());
        Assert.assertEquals(m1.command(), m2.command());
        Assert.assertEquals(m1.type(), m2.type());
        comparePeerAddresses(m1.sender(), m2.sender(), true);
        comparePeerAddresses(m1.recipient(), m2.recipient(), false);
        Assert.assertEquals(m1.payload(), m2.payload());
    }

    private void comparePeerAddresses(final PeerAddress p1, final PeerAddress p2, boolean isSender) {
        Assert.assertEquals(p1.peerId(), p2.peerId());
        if (isSender) {
            TestPeerAddress.compare(p1, p2);
        }
    }
}
