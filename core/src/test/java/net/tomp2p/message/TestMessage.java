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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import net.tomp2p.Utils2;
import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.message.Message.Content;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.cedarsoftware.util.DeepEquals;

/**
 * Tests encoding of an empty message. These tests should not be used for
 * performance measuremenst, since mockito is used, and I guess this would
 * falsify the results.
 * 
 * As a reference, on my laptop IBM T60 its around 190 enc-dec/ms, so we can
 * encode and decode with a single core 192'000 messages per second. For a
 * message smallest size with 59bytes, this means that we can saturate an
 * 86mbit/s link. The larger the message, the more bandwidth it is used
 * (QuadCore~330 enc-dec/ms).
 * 
 * @author Thomas Bocek
 * 
 */
public class TestMessage {

	private static final int SEED = 1;
	private static final int BIT_16 = 256 * 256;
	private static final Random RND = new Random(SEED);
	private static final DSASignatureFactory factory = new DSASignatureFactory();

	@Test
	public void compositeBufferTest1() {
		AlternativeCompositeByteBuf cbuf = AlternativeCompositeByteBuf
				.compBuffer();
		cbuf.writeInt(1);
		ByteBuf buf = Unpooled.buffer();
		buf.writeInt(2);
		cbuf.capacity(4);
		cbuf.addComponent(buf);
		cbuf.writerIndex(8);

		Assert.assertEquals(1, cbuf.readInt());
		Assert.assertEquals(2, cbuf.readInt());
	}

	@Test
	public void compositeBufferTest2() {
		AlternativeCompositeByteBuf cbuf = AlternativeCompositeByteBuf
				.compBuffer();
		int len = 8 * 4;
		for (int i = 0; i < len; i += 4) {
			ByteBuf buf = Unpooled.buffer().writeInt(i);
			cbuf.capacity(cbuf.writerIndex()).addComponent(buf)
					.writerIndex(i + 4);
		}
		cbuf.writeByte(1);

		byte[] me = new byte[len];
		cbuf.readBytes(me);
		cbuf.readByte();

		System.err.println("reader: " + cbuf.readerIndex());
		System.err.println("writer: " + cbuf.writerIndex());
		System.err.println("capacity: " + cbuf.capacity());
		// see https://github.com/netty/netty/issues/1976
		cbuf.discardSomeReadBytes();
	}

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
		List<Number640> tmp2 = new ArrayList<Number640>();
		Number640 n1 = new Number640(rnd);
		Number640 n2 = new Number640(rnd);
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

		Map<Number640, Data> dataMap = new HashMap<Number640, Data>();
		dataMap.put(new Number640(rnd), new Data(new byte[] { 3, 4, 5 }));
		dataMap.put(new Number640(rnd), new Data(new byte[] { 4, 5, 6, 7 }));
		dataMap.put(new Number640(rnd), new Data(new byte[] { 5, 6, 7, 8, 9 }));
		m1.setDataMap(new DataMap(dataMap));
		NavigableMap<Number640, Set<Number160>> keysMap = new TreeMap<Number640, Set<Number160>>();
		Set<Number160> set = new HashSet<Number160>(1);
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		set = new HashSet<Number160>(2);
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		set = new HashSet<Number160>(3);
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		m1.setKeyMap640Keys(new KeyMap640Keys(keysMap));

		Message m2 = encodeDecode(m1);
		Assert.assertEquals(true, m2.getPublicKey(0) != null);
		Assert.assertEquals(false, m2.getDataMap(0) == null);
		Assert.assertEquals(false, m2.getKeyMap640Keys(0) == null);
		Assert.assertEquals(true, m2.verified());
		compareMessage(m1, m2);
	}

	@Test
	public void testEncodeDecode5() throws Exception {
		Message m1 = Utils2.createDummyMessage();
		Random rnd = new Random(42);
		m1.setType(Message.Type.PARTIALLY_OK);
		m1.setHintSign();

		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		m1.setPublicKeyAndSign(pair1);

		Map<Number640, Data> dataMap = new HashMap<Number640, Data>();

		dataMap.put(new Number640(rnd), new Data(new byte[] { 3, 4, 5 }).sign(
				pair1.getPrivate(), factory));
		dataMap.put(new Number640(rnd), new Data(new byte[] { 4, 5, 6, 7 })
				.sign(pair1.getPrivate(), factory));
		dataMap.put(new Number640(rnd), new Data(new byte[] { 5, 6, 7, 8, 9 })
				.sign(pair1.getPrivate(), factory));
		m1.setDataMap(new DataMap(dataMap));
		NavigableMap<Number640, Set<Number160>> keysMap = new TreeMap<Number640, Set<Number160>>();
		Set<Number160> set = new HashSet<Number160>(1);
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		set = new HashSet<Number160>(2);
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		set = new HashSet<Number160>(3);
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		m1.setKeyMap640Keys(new KeyMap640Keys(keysMap));

		Message m2 = encodeDecode(m1);
		Assert.assertEquals(true, m2.getPublicKey(0) != null);
		Assert.assertEquals(false, m2.getDataMap(0) == null);
		Assert.assertEquals(false, m2.getKeyMap640Keys(0) == null);
		Assert.assertEquals(true, m2.verified());
		compareMessage(m1, m2);
	}

	@Test
	public void testEncodeDecode7() throws Exception {
		Message m1 = Utils2.createDummyMessage();
		Random rnd = new Random(42);
		m1.setType(Message.Type.PARTIALLY_OK);
		m1.setHintSign();

		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		m1.setPublicKeyAndSign(pair1);

		Map<Number640, Data> dataMap = new HashMap<Number640, Data>();
		dataMap.put(new Number640(rnd),
				new Data(new byte[] { 3, 4, 5 }).sign(pair1, factory));
		dataMap.put(new Number640(rnd),
				new Data(new byte[] { 4, 5, 6, 7 }).sign(pair1, factory));
		dataMap.put(new Number640(rnd),
				new Data(new byte[] { 5, 6, 7, 8, 9 }).sign(pair1, factory));
		m1.setDataMap(new DataMap(dataMap));
		NavigableMap<Number640, Set<Number160>> keysMap = new TreeMap<Number640, Set<Number160>>();
		Set<Number160> set = new HashSet<Number160>(1);
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		set = new HashSet<Number160>(2);
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		set = new HashSet<Number160>(3);
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		set.add(new Number160(rnd));
		keysMap.put(new Number640(rnd), set);
		m1.setKeyMap640Keys(new KeyMap640Keys(keysMap));

		Message m2 = encodeDecode(m1);
		Assert.assertEquals(true, m2.getPublicKey(0) != null);
		Assert.assertEquals(false, m2.getDataMap(0) == null);
		Assert.assertEquals(false, m2.getDataMap(0).dataMap().entrySet()
				.iterator().next().getValue().signature() == null);
		Assert.assertEquals(false, m2.getDataMap(0).dataMap().entrySet()
				.iterator().next().getValue().publicKey() == null);
		Assert.assertEquals(pair1.getPublic(), m2.getDataMap(0).dataMap()
				.entrySet().iterator().next().getValue().publicKey());
		Assert.assertEquals(false, m2.getKeyMap640Keys(0) == null);
		Assert.assertEquals(true, m2.verified());
		compareMessage(m1, m2);
	}

	@Test
	public void testEncodeDecode6() throws Exception {
		for (int i = 0; i < 4; i++) { // encode and test for is firewallend and
										// ipv4
			Message m1 = Utils2.createDummyMessage((i & 1) > 0, (i & 2) > 0);
			Message m2 = encodeDecode(m1);
			compareMessage(m1, m2);
		}
	}

	@Test
	public void testNumber160Conversion() {
		Number160 i1 = new Number160(
				"0x9908836242582063284904568868592094332017");
		Number160 i2 = new Number160(
				"0x9609416068124319312270864915913436215856");
		Number160 i3 = new Number160(
				"0x7960941606812431931227086491591343621585");
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
		Map<Number640, Data> dataMap = new HashMap<Number640, Data>();
		Data data = new Data(new byte[size]);
		dataMap.put(new Number640(rnd), data);
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
		Map<Number640, Data> dataMap = new HashMap<Number640, Data>(1000);
		Random rnd = new Random(42l);
		for (int i = 0; i < 1000; i++) {
			dataMap.put(new Number640(new Number160(rnd), new Number160(rnd),
					new Number160(rnd), new Number160(rnd)), new Data(
					new byte[] { (byte) rnd.nextInt(), (byte) rnd.nextInt(),
							(byte) rnd.nextInt(), (byte) rnd.nextInt(),
							(byte) rnd.nextInt() }));
		}
		m1.setDataMap(new DataMap(dataMap));
		Message m2 = encodeDecode(m1);
		Assert.assertEquals(true, m2.getPublicKey(0) != null);
		compareMessage(m1, m2);
	}

	@Test
	public void testEncodeDecode480Set() throws Exception { // encode
		Message m1 = Utils2.createDummyMessage();
		m1.setType(Message.Type.NOT_FOUND);
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		m1.setPublicKeyAndSign(pair1);
		Collection<Number640> list = new ArrayList<Number640>();
		Random rnd = new Random(42l);
		for (int i = 0; i < 1000; i++) {
			list.add(new Number640(new Number160(rnd), new Number160(rnd),
					new Number160(rnd), new Number160(rnd)));
		}
		m1.setKeyCollection(new KeyCollection(list));
		Message m2 = encodeDecode(m1);
		Assert.assertEquals(true, m2.getPublicKey(0) != null);
		compareMessage(m1, m2);
	}

	@Test
	public void serializationTest() throws IOException, ClassNotFoundException,
			InvalidKeyException, SignatureException, NoSuchAlgorithmException,
			InvalidKeySpecException {
		Message m1 = Utils2.createDummyMessage();
		m1.setBuffer(new Buffer(Unpooled.buffer()));
		Encoder e = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf
				.compBuffer();
		e.write(buf, m1);
		Decoder d = new Decoder(null);
		boolean header = d.decodeHeader(buf, new InetSocketAddress(0),
				new InetSocketAddress(0));
		boolean payload = d.decodePayload(buf);
		Assert.assertEquals(true, header);
		Assert.assertEquals(true, payload);
		compareMessage(m1, d.message());
	}

	@Test
	public void serializationTestFail() throws IOException,
			ClassNotFoundException, InvalidKeyException, SignatureException {
		try {
			Message m1 = Utils2.createDummyMessage();
			m1.setBuffer(new Buffer(Unpooled.buffer()));
			Utils.encodeJavaObject(m1);
			Assert.fail("Unserializable exception here");
		} catch (Throwable t) {
			// nada
		}
	}

	@Test
	public void testRelayAddresses1() throws Exception { // encode
		Message m1 = Utils2.createDummyMessage();
		m1.setType(Message.Type.NOT_FOUND);
		List<PeerSocketAddress> tmp = new ArrayList<PeerSocketAddress>();
		tmp.add(new PeerSocketAddress(InetAddress.getLocalHost(), 15, 17));
		tmp.add(new PeerSocketAddress(InetAddress.getByName("0:0:0:0:0:0:0:1"),
				16, 18));
		m1.setPeerSocketAddresses(tmp);
		Message m2 = encodeDecode(m1);
		Assert.assertEquals(tmp, m2.getPeerSocketAddresses());
		compareMessage(m1, m2);
	}

	@Test
	public void testRelay() throws Exception {
		Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
		psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.230"),
				RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress
				.getByName("2123:4567:89ab:cdef:0123:4567:89ab:cde2"), RND
				.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.231"),
				RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress
				.getByName("4123:4567:89ab:cdef:0123:4567:89ab:cde4"), RND
				.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.232"),
				RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
		PeerAddress pa3 = new PeerAddress(
				new Number160("0x657435a424444522456"), new PeerSocketAddress(
						InetAddress.getByName("192.168.230.236"),
						RND.nextInt(BIT_16), RND.nextInt(BIT_16)), true, true,
				true, psa);

		Message m1 = Utils2.createDummyMessage();
		Collection<PeerAddress> tmp = new ArrayList<PeerAddress>();
		tmp.add(pa3);
		m1.setNeighborsSet(new NeighborSet(-1, tmp));

		Message m2 = encodeDecode(m1);
		Assert.assertArrayEquals(psa.toArray(), m2.getNeighborsSet(0)
				.neighbors().iterator().next().getPeerSocketAddresses()
				.toArray());
		compareMessage(m1, m2);

	}

	@Test
	public void testRelay2() throws Exception {
		Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
		psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.230"),
				RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress
				.getByName("2123:4567:89ab:cdef:0123:4567:89ab:cde2"), RND
				.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.231"),
				RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress
				.getByName("4123:4567:89ab:cdef:0123:4567:89ab:cde4"), RND
				.nextInt(BIT_16), RND.nextInt(BIT_16)));
		psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.232"),
				RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
		PeerAddress pa3 = new PeerAddress(
				new Number160("0x657435a424444522456"), new PeerSocketAddress(
						InetAddress.getByName("192.168.230.236"),
						RND.nextInt(BIT_16), RND.nextInt(BIT_16)), true, true,
				true, psa);

		Message m1 = Utils2.createDummyMessage();
		Collection<PeerAddress> tmp = new ArrayList<PeerAddress>();
		tmp.add(pa3);
		m1.setNeighborsSet(new NeighborSet(10, tmp));

		Message m2 = encodeDecode(m1);
		Assert.assertEquals(tmp, m2.getPeerSocketAddresses());
		compareMessage(m1, m2);

	}

	// @Test
	// public void testRelayAddresses3() throws Exception { // encode
	// Message m1 = Utils2.createDummyMessage();
	// m1.setType(Message.Type.NOT_FOUND);
	// PeerSocketAddress psa[] = {new
	// PeerSocketAddress(InetAddress.getLocalHost(), 15, 17)};
	// PeerAddress pa = new PeerAddress(Number160.ZERO, new
	// PeerSocketAddress(InetAddress.getByName("localhost"), 13, 14),
	// true, true, true, psa);
	// m1.setSender(pa);
	// Message m2 = encodeDecode(m1);
	// Assert.assertArrayEquals(m1.getSender().getPeerSocketAddresses(),
	// m2.getSender().getPeerSocketAddresses());
	// compareMessage(m1, m2);
	// }

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
		final AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf
				.compBuffer();
		TomP2POutbound encoder = new TomP2POutbound(true,
				new DSASignatureFactory(), new CompByteBufAllocator() {
					@Override
					public AlternativeCompositeByteBuf compBuffer() {
						return buf;
					}

					@Override
					public AlternativeCompositeByteBuf compDirectBuffer() {
						return buf;
					}
				});

		buf.retain();
		ChannelHandlerContext ctx = mockChannelHandlerContext(buf, m2);
		encoder.write(ctx, m1, null);
		Decoder decoder = new Decoder(new DSASignatureFactory());
		decoder.decode(ctx, buf, m1.getRecipient().createSocketTCP(), m1
				.getSender().createSocketTCP());
		return decoder.message();
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
	private ChannelHandlerContext mockChannelHandlerContext(
			final AlternativeCompositeByteBuf buf,
			final AtomicReference<Message> m2) {
		ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
		ByteBufAllocator alloc = mock(ByteBufAllocator.class);
		when(ctx.alloc()).thenReturn(alloc);
		when(alloc.ioBuffer()).thenReturn(buf);

		DatagramChannel dc = mock(DatagramChannel.class);
		when(ctx.channel()).thenReturn(dc);
		when(ctx.writeAndFlush(any(), any(ChannelPromise.class))).thenReturn(
				null);

		Attribute<InetSocketAddress> attr = mock(Attribute.class);
		when(ctx.attr(any(AttributeKey.class))).thenReturn(attr);

		when(ctx.fireChannelRead(any())).then(new Answer<Void>() {
			@Override
			public Void answer(final InvocationOnMock invocation)
					throws Throwable {
				Object[] args = invocation.getArguments();
				m2.set((Message) args[0]);
				return null;
			}
		});

		when(ctx.fireExceptionCaught(any(Throwable.class))).then(
				new Answer<Void>() {

					@Override
					public Void answer(InvocationOnMock invocation)
							throws Throwable {
						Object[] args = invocation.getArguments();
						for (Object obj : args) {
							if (obj instanceof Throwable) {
								((Throwable) obj).printStackTrace();
							} else {
								System.err.println("Err: " + obj);
							}
						}
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
		Assert.assertEquals(m1.getSender().tcpPort(), m2.getSender().tcpPort());

		Assert.assertEquals(
				true,
				Utils.isSameSets(m1.getBloomFilterList(),
						m2.getBloomFilterList()));
		Assert.assertEquals(true,
				Utils.isSameSets(m1.getBufferList(), m2.getBufferList()));

		Assert.assertEquals(m1.getDataMapList().size(), m2.getDataMapList()
				.size());

		// ;
		for (Iterator<DataMap> iter1 = m1.getDataMapList().iterator(), iter2 = m2
				.getDataMapList().iterator(); iter1.hasNext()
				&& iter2.hasNext();) {
			Assert.assertEquals(true, DeepEquals.deepEquals(iter1.next()
					.dataMap(), iter2.next().dataMap()));
		}

		Assert.assertEquals(true,
				Utils.isSameSets(m1.getIntegerList(), m2.getIntegerList()));
		Assert.assertEquals(true,
				Utils.isSameSets(m1.getKeyList(), m2.getKeyList()));
		Assert.assertEquals(
				true,
				Utils.isSameSets(m1.getKeyCollectionList(),
						m2.getKeyCollectionList()));
		Assert.assertEquals(
				true,
				Utils.isSameSets(m1.getKeyMapKeys640List(),
						m2.getKeyMapKeys640List()));
		Assert.assertEquals(true,
				Utils.isSameSets(m1.getLongList(), m2.getLongList()));

		Assert.assertEquals(m1.getNeighborsSetList().size(), m2
				.getNeighborsSetList().size());
		for (Iterator<NeighborSet> iter1 = m1.getNeighborsSetList().iterator(), iter2 = m2
				.getNeighborsSetList().iterator(); iter1.hasNext()
				&& iter2.hasNext();) {
			Assert.assertEquals(true, Utils.isSameSets(
					iter1.next().neighbors(), iter2.next().neighbors()));
		}

		Assert.assertEquals(m1.getSender().isFirewalledTCP(), m2.getSender()
				.isFirewalledTCP());
		Assert.assertEquals(m1.getSender().isFirewalledUDP(), m2.getSender()
				.isFirewalledUDP());
	}

	/**
	 * Create the content types of messages. If a content type is null or Empty,
	 * this is the same.
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
