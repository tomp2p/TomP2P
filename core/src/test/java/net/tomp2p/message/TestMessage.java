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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import net.tomp2p.Utils2;
import net.tomp2p.connection2.DefaultSignatureFactory;
import net.tomp2p.message.Message.Content;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests encoding of an empty message. These tests should not be used for performance measuremenst, since mockito is
 * used, and I guess this would falsify the results.
 * 
 * As a reference, on my laptop IBM T60 its around 190 enc-dec/ms, so we can encode and decode with a single core
 * 192'000 messages per second. For a message smallest size with 59bytes, this means that we can saturate an 86mbit/s
 * link. The larger the message, the more bandwidth it is used (QuadCore~330 enc-dec/ms).
 * 
 * @author Thomas Bocek
 * 
 */
public class TestMessage {

    /**
     * Test a simple message to endcode and then decode.
     * 
     * @throws Exception .
     */
    @Test
    public void testEncodeDecode() throws Exception {
        // encode
        Message m1 = Utils2.createDummyMessage();
        Message m2 = encodeDecode(m1);
        compareMessage(m1, m2);
    }

    /**
     * Tests a different command, type and integer.
     * 
     * @throws Exception .
     */
    @Test
    public void testEncodeDecode2() throws Exception { // encode
        Random rnd = new Random(42);
        Message m1 = Utils2.createDummyMessage();
        m1.setCommand((byte) 3);
        m1.setType(Message.Type.DENIED);
        Number160 key1 = new Number160(5667);
        Number160 key2 = new Number160(5667);
        m1.setKey(key1);
        m1.setKey(key2);
        List<Number480> tmp2 = new ArrayList<Number480>();
        Number480 n1 = new Number480(rnd);
        Number480 n2 = new Number480(rnd);
        tmp2.add(n1);
        tmp2.add(n2);
        m1.setKeyCollection(new KeyCollection(tmp2));

        Message m2 = encodeDecode(m1);

        Assert.assertEquals(false, m2.getKeyList() == null);
        Assert.assertEquals(false, m2.getKeyCollectionList() == null);
        compareMessage(m1, m2);
    }

    /**
     * Tests Number160 and string.
     * 
     * @throws Exception .
     */
    @Test
    public void testEncodeDecode3() throws Exception { // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType(Message.Type.DENIED);
        m1.setLong(8888888);
        byte[] me = new byte[10000];
        ByteBuf tmp = Unpooled.wrappedBuffer(me);
        m1.setBuffer(new Buffer(tmp));
        Message m2 = encodeDecode(m1);
        Assert.assertEquals(false, m2.getBuffer(0) == null);
        compareMessage(m1, m2);
    }

    /**
     * Tests neighbors and payload.
     * 
     * @throws Exception .
     */
    @Test
    public void testEncodeDecode4() throws Exception {
        Message m1 = Utils2.createDummyMessage();
        Random rnd = new Random(42);
        m1.setType(Message.Type.DENIED);
        m1.setHintSign();

        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        m1.setPublicKeyAndSign(pair1);

        Map<Number480, Data> dataMap = new HashMap<Number480, Data>();
        dataMap.put(new Number480(rnd), new Data(new byte[] { 3, 4, 5 }, true, true));
        dataMap.put(new Number480(rnd), new Data(new byte[] { 4, 5, 6 }, true, true));
        dataMap.put(new Number480(rnd), new Data(new byte[] { 5, 6, 7 }, true, true));
        m1.setDataMap(new DataMap(dataMap));
        Map<Number480, Number160> keysMap = new HashMap<Number480, Number160>();
        keysMap.put(new Number480(rnd), new Number160(rnd));
        keysMap.put(new Number480(rnd), new Number160(rnd));
        keysMap.put(new Number480(rnd), new Number160(rnd));
        m1.setKeyMap480(new KeyMap480(keysMap));
        //

        Message m2 = encodeDecode(m1);
        Assert.assertEquals(true, m2.getPublicKey() != null);
        Assert.assertEquals(false, m2.getDataMap(0) == null);
        Assert.assertEquals(false, m2.getKeyMap480(0) == null);
        compareMessage(m1, m2);
    }

    @Test
    public void testEncodeDecode6() throws Exception {
        for (int i = 0; i < 4; i++) { // encode and test for is firewallend and ipv4
            Message m1 = Utils2.createDummyMessage((i & 1) > 0, (i & 2) > 0);
            Message m2 = encodeDecode(m1);
            compareMessage(m1, m2);
        }
    }

    @Test
    public void testNumber160Conversion() {
        Number160 i1 = new Number160("0x9908836242582063284904568868592094332017");
        Number160 i2 = new Number160("0x9609416068124319312270864915913436215856");
        Number160 i3 = new Number160("0x7960941606812431931227086491591343621585");
        byte[] me = i1.toByteArray();
        Assert.assertEquals(i1, new Number160(me));
        me = i2.toByteArray();
        Assert.assertEquals(i2, new Number160(me));
        me = i3.toByteArray();
        byte[] me2 = new byte[20];
        System.arraycopy(me, 0, me2, me2.length - me.length, me.length);
        Assert.assertEquals(i3, new Number160(me2));
    }

    @Test
    public void testBigData() throws Exception {
        final int size = 50 * 1024 * 1024;
        Random rnd = new Random(42);
        Message m1 = Utils2.createDummyMessage();
        Map<Number480, Data> dataMap = new HashMap<Number480, Data>();
        Data data = new Data(new byte[size], true, false);
        dataMap.put(new Number480(rnd), data);
        m1.setDataMap(new DataMap(dataMap));
        Message m2 = encodeDecode(m1);
        compareMessage(m1, m2);
    }

    @Test
    public void testEncodeDecode480Map() throws Exception { // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType(Message.Type.PARTIALLY_OK);
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        m1.setPublicKeyAndSign(pair1);
        Map<Number480, Data> dataMap = new HashMap<Number480, Data>(1000);
        Random rnd = new Random(42l);
        for (int i = 0; i < 1000; i++) {
            dataMap.put(new Number480(new Number160(rnd), new Number160(rnd), new Number160(rnd)),
                    new Data(new byte[] { (byte) rnd.nextInt(), (byte) rnd.nextInt(), (byte) rnd.nextInt(),
                            (byte) rnd.nextInt(), (byte) rnd.nextInt() }, true, true));
        }
        m1.setDataMap(new DataMap(dataMap));
        Message m2 = encodeDecode(m1);
        Assert.assertEquals(true, m2.getPublicKey() != null);
        compareMessage(m1, m2);
    }

    @Test
    public void testEncodeDecode480Set() throws Exception { // encode
        Message m1 = Utils2.createDummyMessage();
        m1.setType(Message.Type.NOT_FOUND);
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        m1.setPublicKeyAndSign(pair1);
        Collection<Number480> list = new ArrayList<Number480>();
        Random rnd = new Random(42l);
        for (int i = 0; i < 1000; i++) {
            list.add(new Number480(new Number160(rnd), new Number160(rnd), new Number160(rnd)));
        }
        m1.setKeyCollection(new KeyCollection(list));
        Message m2 = encodeDecode(m1);
        Assert.assertEquals(true, m2.getPublicKey() != null);
        compareMessage(m1, m2);
    }

    /**
     * Encodes and decodes a message.
     * 
     * @param m1
     *            The message the will be encoded
     * @return The message that was decoded.
     * @throws Exception .
     */
    private Message encodeDecode(final Message m1) throws Exception {
        AtomicReference<Message> m2 = new AtomicReference<Message>();
        TomP2POutbound encoder = new TomP2POutbound(true, new DefaultSignatureFactory());
        ByteBuf buf = Unpooled.buffer();
        ChannelHandlerContext ctx = mockChannelHandlerContext(buf, m2);
        encoder.write(ctx, m1, null);
        TomP2PDecoder decoder = new TomP2PDecoder(new DefaultSignatureFactory());
        decoder.decode(ctx, buf, m1.getRecipient().createSocketTCP(), m1.getSender().createSocketTCP());
        return m2.get();
    }

    /**
     * Mock Nettys ChannelHandlerContext with the minimal functions.
     * 
     * @param buf
     *            The buffer to use for decoding
     * @param m2
     *            The message reference to store the result
     * @return The mocked ChannelHandlerContext
     */
    @SuppressWarnings("unchecked")
    private ChannelHandlerContext mockChannelHandlerContext(final ByteBuf buf,
            final AtomicReference<Message> m2) {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ByteBufAllocator alloc = mock(ByteBufAllocator.class);
        when(ctx.alloc()).thenReturn(alloc);
        when(alloc.ioBuffer()).thenReturn(buf);
        DatagramChannel dc = mock(DatagramChannel.class);
        when(ctx.channel()).thenReturn(dc);
        when(ctx.writeAndFlush(any(), any(ChannelPromise.class))).thenReturn(null);

        Attribute<InetSocketAddress> attr = mock(Attribute.class);
        when(ctx.attr(any(AttributeKey.class))).thenReturn(attr);

        when(ctx.fireChannelRead(any())).then(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                m2.set((Message) args[0]);
                return null;
            }
        });

        return ctx;
    }

    /**
     * Checks if two messages are the same.
     * 
     * @param m1
     *            The first message
     * @param m2
     *            The second message
     */
    private void compareMessage(final Message m1, final Message m2) {
        Assert.assertEquals(m1.getMessageId(), m2.getMessageId());
        Assert.assertEquals(m1.getVersion(), m2.getVersion());
        Assert.assertEquals(m1.getCommand(), m2.getCommand());
        compareContentTypes(m1, m2);
        Assert.assertEquals(m1.getRecipient(), m2.getRecipient());
        Assert.assertEquals(m1.getType(), m2.getType());
        Assert.assertEquals(m1.getSender(), m2.getSender());

        Assert.assertEquals(true, Utils.isSameSets(m1.getBloomFilterList(), m2.getBloomFilterList()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getBufferList(), m2.getBufferList()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getDataMapList(), m2.getDataMapList()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getIntegerList(), m2.getIntegerList()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getKeyList(), m2.getKeyList()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getKeyCollectionList(), m2.getKeyCollectionList()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getKeyMap480List(), m2.getKeyMap480List()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getLongList(), m2.getLongList()));
        Assert.assertEquals(true, Utils.isSameSets(m1.getNeighborsSetList(), m2.getNeighborsSetList()));

        Assert.assertEquals(m1.getSender().isFirewalledTCP(), m2.getSender().isFirewalledTCP());
        Assert.assertEquals(m1.getSender().isFirewalledUDP(), m2.getSender().isFirewalledUDP());
    }

    /**
     * Create the content types of messages. If a content type is null or Empty, this is the same.
     * 
     * @param m1
     *            The first message
     * @param m2
     *            The second message
     */
    private void compareContentTypes(final Message m1, final Message m2) {
        for (int i = 0; i < m1.getContentTypes().length; i++) {
            Content type1 = m1.getContentTypes()[i];
            Content type2 = m2.getContentTypes()[i];
            if (type1 == null) {
                type1 = Content.EMPTY;
            }
            if (type2 == null) {
                type2 = Content.EMPTY;
            }
            Assert.assertEquals(type1, type2);

        }
    }
}
