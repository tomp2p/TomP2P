package net.tomp2p.message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import net.tomp2p.Utils2;
import net.tomp2p.message.Message.Command;
import net.tomp2p.p2p.P2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapKadImpl;
import net.tomp2p.storage.Data;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;


public class TestMessage
{
	/**
	 * Tests encoding of an empty message
	 */
	
	@Test
	public void testEncodeDecode() throws Exception
	{
		testEncodeDecode(true);
		testEncodeDecode(false);
	}
	
	private void testEncodeDecode(boolean tcp) throws Exception
	{
		// setup
		SocketAddress sockRemote = new InetSocketAddress(2000);
		SocketAddress sockLocal = new InetSocketAddress(1000);
		DummyChannel dc = new DummyChannel(sockRemote, sockLocal);
		// encode
		Message m1 = Utils2.createDummyMessage();
		DummyCoder coder=new DummyCoder(tcp);
		ChannelBuffer buffer = coder.encode(m1);
		// decode
		Object obj = new TomP2PDecoderTCP().decode(null, dc, buffer);
		// test
		Assert.assertTrue(obj instanceof Message);
		Message m2 = (Message) obj;
		compareMessage(m1, m2);
	}

	/**
	 * Tests a different command, type and integer
	 */
	
	@Test
	public void testEncodeDecode2() throws Exception
	{
		testEncodeDecode2(true);
		testEncodeDecode2(false);
	}
	
	private void testEncodeDecode2(boolean tcp) throws Exception
	{
		// setup
		SocketAddress sockRemote = new InetSocketAddress(2000);
		SocketAddress sockLocal = new InetSocketAddress(1000);
		DummyChannel dc = new DummyChannel(sockRemote, sockLocal);
		// encode
		Message m1 = Utils2.createDummyMessage();
		m1.setCommand(Command.NEIGHBORS_TRACKER);
		m1.setType(Message.Type.DENIED);
		Number160 key1 = new Number160(5667);
		Number160 key2 = new Number160(5667);
		m1.setKeyKey(key1, key2);
		List<Number160> tmp2 = new ArrayList<Number160>();
		tmp2.add(new Number160("0x234567890"));
		tmp2.add(new Number160("0x77"));
		m1.setKeys(tmp2);
		
		DummyCoder coder=new DummyCoder(tcp);
		ChannelBuffer buffer = coder.encode(m1);
		// decode
		Object obj = new TomP2PDecoderTCP().decode(null, dc, buffer);
		// test
		Message m2 = (Message) obj;
		Assert.assertEquals(false, m2.getKey1() == null);
		Assert.assertEquals(false, m2.getKeys() == null);
		compareMessage(m1, m2);

	}

	@Test
	public void testEncodeDecode3() throws Exception
	{
		testEncodeDecode3(true);
		testEncodeDecode3(false);
	}
	
	/**
	 * Tests Number160 and string
	 */
	private void testEncodeDecode3(boolean tcp) throws Exception
	{
		// setup
		SocketAddress sockRemote = new InetSocketAddress(2000);
		SocketAddress sockLocal = new InetSocketAddress(1000);
		DummyChannel dc = new DummyChannel(sockRemote, sockLocal);
		// encode
		Message m1 = Utils2.createDummyMessage();
		m1.setType(Message.Type.DENIED);
		m1.setLong(8888888);
		byte[] me = new byte[10000];
		ChannelBuffer tmp=ChannelBuffers.wrappedBuffer(me);
		m1.setPayload(tmp);
		DummyCoder coder=new DummyCoder(tcp);
		ChannelBuffer buffer = coder.encode(m1);
		// decode
		Object obj = new TomP2PDecoderTCP().decode(null, dc, buffer);
		// test
		Message m2 = (Message) obj;
		Assert.assertEquals(false, m2.getPayload1() == null);
		compareMessage(m1, m2);

	}
	
	/**
	 * Tests neighbors and payload
	 */
	@Test
	public void testEncodeDecode4() throws Exception
	{
		testEncodeDecode4(true);
		testEncodeDecode4(false);
	}
	
	private void testEncodeDecode4(boolean tcp) throws Exception
	{
		// setup
		DummyCoder coder=new DummyCoder(tcp);
		// encode
		Message m1 = Utils2.createDummyMessage();
		m1.setType(Message.Type.DENIED);
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		m1.setPublicKeyAndSign(pair1);
		Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
		dataMap.put(new Number160(45), new Data(new byte[] { 3, 4, 5 }));
		dataMap.put(new Number160(46), new Data(new byte[] { 4, 5, 6 }));
		Data data=new Data(new byte[] { 5, 6, 7 });
		data.setProtectedEntry(true);
		dataMap.put(new Number160(47), data);
		m1.setDataMap(dataMap);
		Map<Number160, Number160> keyMap = new HashMap<Number160, Number160>();
		keyMap.put(new Number160(55), new Number160(66));
		keyMap.put(new Number160(551), new Number160(661));
		keyMap.put(new Number160(5511), new Number160(66111));
		m1.setKeyMap(keyMap);
		
		Message m2 = coder.decode(coder.encode(m1));
		// test
		Assert.assertEquals(false, m2.getDataMap() == null);
		Assert.assertEquals(false, m2.getKeyMap() == null);
		compareMessage(m1, m2);
	}
	
	@Test
	public void testEncodeDecode6() throws Exception
	{
		testEncodeDecode6(true);
		testEncodeDecode6(false);
	}

	private void testEncodeDecode6(boolean tcp) throws Exception
	{
		for(int i=0;i<4;i++)
		{
			// setup
			SocketAddress sockRemote = new InetSocketAddress(2000);
			SocketAddress sockLocal = new InetSocketAddress(1000);
			DummyChannel dc = new DummyChannel(sockRemote, sockLocal);
			// encode and test for is firewallend and ipv4
			Message m1 = Utils2.createDummyMessage((i & 1) > 0, (i & 2) > 0);
			m1.setType(Message.Type.DENIED);
			DummyCoder coder=new DummyCoder(tcp);
			ChannelBuffer buffer = coder.encode(m1);
			// decode
			Object obj = new TomP2PDecoderTCP().decode(null, dc, buffer);
			// test
			Message m2 = (Message) obj;
			Assert.assertEquals(m1.getSender().isFirewalledTCP(), m2.getSender().isFirewalledTCP());
			Assert.assertEquals(m1.getSender().isFirewalledUDP(), m2.getSender().isFirewalledUDP());
			compareMessage(m1, m2);
		}
	}

	/**
	 * Test encode decode performance. On my laptop IBM T60 its around 190
	 * enc-dec/ms, so we can encode and decode with a single core 192'000
	 * messages per second. For a message smallest size with 59bytes, this means
	 * that we can saturate an 86mbit/s link. The larger the message, the more
	 * bandwidth we use.
	 * 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPerformanceTCPEncoder() throws Exception
	{
		// setup
		SocketAddress sockRemote = new InetSocketAddress(2000);
		SocketAddress sockLocal = new InetSocketAddress(1000);
		DummyChannel dc = new DummyChannel(sockRemote, sockLocal);
		// encode
		Message m1 = Utils2.createDummyMessage();
		m1.setType(Message.Type.UNKNOWN_ID);
		List<Number160> tmp = new ArrayList<Number160>();
		tmp.add(new Number160("0x1234567890"));
		tmp.add(new Number160("0x777"));
		m1.setKeys(tmp);
		// System.in.read();ChannelBuffer
		long start = System.currentTimeMillis();
		int len = 5000000;
		ChannelBuffer buffer = null;
		DummyCoder coder=new DummyCoder(true);
		TomP2PDecoderTCP dec=new TomP2PDecoderTCP();
		for (int i = 0; i < len; i++)
		{
			// encode
			buffer = coder.encode(m1);
			// decode
			dec.decode(null, dc, buffer);
		}
		long stop = System.currentTimeMillis() - start;
		System.out.println("Performance: " + (len / stop) + " enc-dec/ms (reference: QuadCore~330 enc-dec/ms)");
	}
	private void compareMessage(Message m1, Message m2)
	{
		Assert.assertEquals(m1.getMessageId(), m2.getMessageId());
		Assert.assertEquals(m1.getVersion(), m2.getVersion());
		Assert.assertEquals(m1.getCommand(), m2.getCommand());
		Assert.assertEquals(m1.getContentType1(), m2.getContentType1());
		Assert.assertEquals(m1.getContentType2(), m2.getContentType2());
		Assert.assertEquals(m1.getRecipient(), m2.getRecipient());
		Assert.assertEquals(m1.getType(), m2.getType());
		Assert.assertEquals(m1.getSender(), m2.getSender());
		//
		if(m1.getNeighbors()!=null && m2.getNeighbors()!=null)
		{
			Assert.assertEquals(m1.getNeighbors().size(), m2.getNeighbors().size());
			Iterator<PeerAddress> it1 = m1.getNeighbors().iterator();
			Iterator<PeerAddress> it2 = m2.getNeighbors().iterator();
			while(it1.hasNext() && it2.hasNext())
			{
				PeerAddress p1 = it1.next();
				PeerAddress p2 = it2.next();
				Assert.assertEquals(p1, p2);
				Assert.assertEquals(p1.createSocketTCP(), p2.createSocketTCP());
				Assert.assertEquals(p1.createSocketUDP(), p2.createSocketUDP());
			}
		}
		if (m1.getKeys() != null && m2.getKeys() != null)
		{
			Assert.assertEquals(m1.getKeys().size(), m2.getKeys().size());
			Iterator<Number160> it1 = m1.getKeys().iterator();
			Iterator<Number160> it2 = m2.getKeys().iterator();
			while (it1.hasNext() && it2.hasNext())
			{
				Number160 key1 = it1.next();
				Number160 key2 = it2.next();
				Assert.assertEquals(key1, key2);
			}
		}
		//
		if (m1.getDataMap() != null && m2.getDataMap() != null)
		{
			Assert.assertEquals(m1.getDataMap().size(), m2.getDataMap().size());
			Iterator<Number160> it1 = m1.getDataMap().keySet().iterator();
			Iterator<Number160> it2 = m2.getDataMap().keySet().iterator();
			while (it1.hasNext() && it2.hasNext())
			{
				Number160 key1 = it1.next();
				Number160 key2 = it2.next();
				Assert.assertEquals(key1, key2);
				// Assert.assertEquals(m1.dataMap().get(key1),
				// m2.dataMap().get(key2));
				Assert.assertEquals(m1.getDataMap().get(key1).getLength(), m2.getDataMap().get(key2)
						.getLength());
				Data d1 = m1.getDataMap().get(key1);
				Data d2 = m2.getDataMap().get(key2);
				for (int i = 0; i < d1.getLength(); i++)
				{
					Assert.assertEquals(d1.getData()[d1.getOffset() + i], d2.getData()[d2
							.getOffset()
							+ i]);
				}
			}
		}
		if (m1.getKeyMap() != null && m2.getKeyMap() != null)
		{
			Assert.assertEquals(m1.getKeyMap().size(), m2.getKeyMap().size());
			Iterator<Number160> it1 = m1.getKeyMap().keySet().iterator();
			Iterator<Number160> it2 = m2.getKeyMap().keySet().iterator();
			while (it1.hasNext() && it2.hasNext())
			{
				Number160 key1 = it1.next();
				Number160 key2 = it2.next();
				Assert.assertEquals(key1, key2);
				Assert.assertEquals(m1.getKeyMap().get(key1), m2.getKeyMap().get(key2));
			}
		}
		Assert.assertEquals(m1.getKey1(), m2.getKey1());
		Assert.assertEquals(m1.getKey2(), m2.getKey2());
		Assert.assertEquals(m1.getLong(), m2.getLong());
		if (m1.getPayload1() != null && m2.getPayload1() != null)
		{
			//compare payload.. how?
		}
	}

	@Test
	public void testOrder()
	{
		Number160 b1 = new Number160("0x5");
		Number160 b2 = new Number160("0x32");
		Number160 b3 = new Number160("0x1F4");
		Number160 b4 = new Number160("0x1388");
		PeerAddress n1 = new PeerAddress(b1);
		PeerAddress n2 = new PeerAddress(b2);
		PeerAddress n3 = new PeerAddress(b3);
		PeerAddress n4 = new PeerAddress(b4);
		
		P2PConfiguration conf= new P2PConfiguration();
		conf.setBagSize(2);
		conf.setCacheSize(100);
		conf.setCacheTimeoutMillis(60*1000);
		conf.setMaxNrBeforeExclude(3);
		PeerMap routingMap = new PeerMapKadImpl(b1, conf);
		final NavigableSet<PeerAddress> queue = new TreeSet<PeerAddress>(routingMap
				.createPeerComparator(b3));
		queue.add(n1);
		queue.add(n2);
		queue.add(n3);
		queue.add(n4);
		Assert.assertEquals(queue.pollFirst(), n3);
		Assert.assertEquals(queue.pollFirst(), n2);
		Assert.assertEquals(queue.pollLast(), n4);
	}

	@Test
	public void testNumber160Conversion()
	{
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
}
