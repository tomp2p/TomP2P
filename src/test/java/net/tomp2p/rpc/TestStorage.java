package net.tomp2p.rpc;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureSuccessEvaluatorCommunication;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.ShortString;
import net.tomp2p.replication.Replication;
import net.tomp2p.replication.ResponsibilityListener;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageDisk;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestStorage
{
	final private static Number160 domainKey = new Number160(20);
	private static String DIR1;
	private static String DIR2;
		
	@Before
	public void before() throws IOException
	{
		DIR1 = Utils2.createTempDirectory().getCanonicalPath();
		DIR2 = Utils2.createTempDirectory().getCanonicalPath();
	}

	@After
	public void after()
	{
		cleanUp(DIR1);
		cleanUp(DIR2);
	}
	
	private void cleanUp(String dir)
	{
		File f = new File(dir);
		f.listFiles(new FileFilter()
		{
			@Override
			public boolean accept(File pathname)
			{
				if (pathname.isFile())
					pathname.delete();
				return false;
			}
		});
		f.delete();
	}
	
	@Test
	public void testAdd() throws Exception
	{
		testAdd(new StorageMemory(), new StorageMemory());
		testAdd(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}
	
	private void testAdd(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			
			Collection<Data> dataSet = new HashSet<Data>();
			dataSet.add(new Data(1));
			FutureResponse fr = smmSender.add(recv1.getPeerAddress(), new Number160(33), new ShortString("test").toNumber160(), 
					dataSet, false, false, true, cc, false);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			// add a the same data twice
			fr = smmSender.add(recv1.getPeerAddress(), new Number160(33), new ShortString("test").toNumber160(), 
					dataSet, false, false, true, cc, false);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			
			Number320 key=new Number320(new Number160(33), new ShortString("test").toNumber160());
			//Set<Number480> tofetch = new HashSet<Number480>();
			SortedMap<Number480, Data> c = storeRecv.get(key.getLocationKey(), key.getDomainKey(), Number160.ZERO, Number160.MAX_VALUE);
			Assert.assertEquals(2, c.size());
			for(Data data:c.values())
			{
				Assert.assertEquals((Integer)1, (Integer)data.getObject());
			}
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		catch (Throwable e)
		{
			e.printStackTrace();
			Assert.fail();
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}

	@Test
	public void testStorePut() throws Exception
	{
		testStorePut(new StorageMemory(), new StorageMemory());
		testStorePut(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}
	private void testStorePut(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			System.err.println(recv1.getPeerAddress());
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			
			Number320 key=new Number320(new Number160(33), new ShortString("test")
			.toNumber160());
			//Set<Number480> tofetch = new HashSet<Number480>();
			Data c = storeRecv.get(key.getLocationKey(), key.getDomainKey(), new Number160(77));
			for (int i = 0; i < me1.length; i++)
				Assert.assertEquals(me1[i], c.getData()[i + c.getOffset()]);
			//
			tmp.clear();
			me1 = new byte[] { 5, 6, 7 };
			me2 = new byte[] { 8, 9, 1, 5 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			fr = smmSender.put(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			Map<Number480, Data>  result2 = storeRecv.subMap(key.getLocationKey(), key.getDomainKey(), Number160.ZERO, Number160.MAX_VALUE);
			Assert.assertEquals(result2.size(), 2);
			Number480 search=new Number480(key, new Number160(88));
			c = result2.get(search);
			for (int i = 0; i < me2.length; i++)
				Assert.assertEquals(me2[i], c.getData()[i + c.getOffset()]);
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		catch (Throwable e)
		{
			e.printStackTrace();
			Assert.fail();
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testComparePut() throws Exception
	{
		testComparePut(new StorageMemory(), new StorageMemory());
		testComparePut(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}
	private void testComparePut(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			Data d1=new Data(me1);
			Data d2=new Data(me2);
			tmp.put(new Number160(77), d1);
			tmp.put(new Number160(88), d2);
			System.err.println(recv1.getPeerAddress());
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			
			Number320 key=new Number320(new Number160(33), new ShortString("test")
			.toNumber160());
			//Set<Number480> tofetch = new HashSet<Number480>();
			Data c = storeRecv.get(key.getLocationKey(), key.getDomainKey(), new Number160(77));
			for (int i = 0; i < me1.length; i++)
				Assert.assertEquals(me1[i], c.getData()[i + c.getOffset()]);
			//
			tmp.clear();
			me1 = new byte[] { 5, 6, 7 };
			me2 = new byte[] { 8, 9, 1, 5 };
			
			Map<Number160, HashData> hashDataMap=new HashMap<Number160, HashData>();
			hashDataMap.put(new Number160(77), new HashData(d1.getHash(), new Data(me1)));
			hashDataMap.put(new Number160(88), new HashData(new Number160(33), new Data(me2)));
			
			fr = smmSender.compareAndPut(recv1.getPeerAddress(), new Number160(33), new ShortString("test").toNumber160(), 
					hashDataMap, new FutureSuccessEvaluatorCommunication(), false, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			//we fail because we provided a false hash
			Assert.assertEquals(false, fr.isSuccess() && fr.getResponse().isOk());
			System.err.println(fr.getFailedReason());
			
			//set the correct hash
			hashDataMap.put(new Number160(88), new HashData(d2.getHash(), new Data(me2)));
			fr = smmSender.compareAndPut(recv1.getPeerAddress(), new Number160(33), new ShortString("test").toNumber160(), 
					hashDataMap, new FutureSuccessEvaluatorCommunication(), false, false, false, false, cc, false);
			
			fr.awaitUninterruptibly();
			sender.getConnectionBean().getConnectionReservation().release(cc);
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess() && fr.getResponse().isOk());
			Map<Number480, Data>  result2 = storeRecv.subMap(key.getLocationKey(), key.getDomainKey(), Number160.ZERO, Number160.MAX_VALUE);
			Assert.assertEquals(result2.size(), 2);
			Number480 search=new Number480(key, new Number160(88));
			c = result2.get(search);
			for (int i = 0; i < me2.length; i++)
				Assert.assertEquals(me2[i], c.getData()[i + c.getOffset()]);
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.fail();
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testStorePutIfAbsent() throws Exception
	{
		testStorePutIfAbsent(new StorageMemory(), new StorageMemory());
		testStorePutIfAbsent(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testStorePutIfAbsent(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Data c = storeRecv.get(new Number160(33), new ShortString("test")
			.toNumber160(), new Number160(77));
			for (int i = 0; i < me1.length; i++)
				Assert.assertEquals(me1[i], c.getData()[i + c.getOffset()]);
			//
			tmp.clear();
			byte[] me3 = new byte[] { 5, 6, 7 };
			byte[] me4 = new byte[] { 8, 9, 1, 5 };
			tmp.put(new Number160(77), new Data(me3));
			tmp.put(new Number160(88), new Data(me4));
			fr = smmSender.putIfAbsent(recv1.getPeerAddress(), new Number160(33), new ShortString(
					"test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			// we cannot put anything there, since there already is 
			Assert.assertEquals(true, fr.isSuccess());
			Collection<Number160> putKeys = fr.getResponse().getKeys();
			Assert.assertEquals(0, putKeys.size());
			c = storeRecv.get(new Number160(33), new ShortString("test").toNumber160(),new Number160(88));
			for (int i = 0; i < me2.length; i++)
				Assert.assertEquals(me2[i], c.getData()[i + c.getOffset()]);
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testStorePutGet() throws Exception
	{
		testStorePutGet(new StorageMemory(), new StorageMemory());
		testStorePutGet(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testStorePutGet(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp.keySet(), null, null,null, false,false,false,  cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			compare(tmp, stored);
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testStorePutGet2() throws Exception
	{
		testStorePutGet2(new StorageMemory(), new StorageMemory());
		testStorePutGet2(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	
	private void testStorePutGet2(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, null,null, false, false,false, cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			compare(tmp, stored);
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}

	private void compare(Map<Number160, Data> tmp, Map<Number160, Data> stored)
	{
		Assert.assertEquals(tmp.size(), stored.size());
		Iterator<Number160> iterator1 = tmp.keySet().iterator();
		Iterator<Number160> iterator2 = stored.keySet().iterator();
		while (iterator1.hasNext() && iterator2.hasNext())
		{
			Number160 key1 = iterator1.next();
			Number160 key2 = iterator2.next();
			Assert.assertEquals(key1, key2);
			Data data1 = tmp.get(key1);
			Data data2 = stored.get(key2);
			Assert.assertEquals(data1.getLength(), data2.getLength());
			for (int i = 0; i < data1.getLength(); i++)
			{
				Assert.assertEquals(data1.getData()[data1.getOffset() + i], data2.getData()[data2
						.getOffset()
						+ i]);
			}
		}
	}
	
	@Test
	public void testStorePutRemoveGet() throws Exception
	{
		testStorePutRemoveGet(new StorageMemory(), new StorageMemory());
		testStorePutRemoveGet(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testStorePutRemoveGet(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			// remove
			fr = smmSender.remove(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp.keySet(), true, false, cc, false);
			fr.awaitUninterruptibly();
			Message m = fr.getResponse();
			Assert.assertEquals(true, fr.isSuccess());
			Map<Number160, Data> removed = m.getDataMap();
			compare(tmp, removed);
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp.keySet(), null, null,null, false, false,false, cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			Assert.assertEquals(0, stored.size());
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testStorePutRemoveGet2() throws Exception
	{
		testStorePutRemoveGet2(new StorageMemory(), new StorageMemory());
		testStorePutRemoveGet2(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testStorePutRemoveGet2(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			// remove
			fr = smmSender.remove(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), null, true, false, cc, false);
			fr.awaitUninterruptibly();
			Message m = fr.getResponse();
			Assert.assertEquals(true, fr.isSuccess());
			Map<Number160, Data> removed = m.getDataMap();
			compare(tmp, removed);
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp.keySet(), null, null,null, false, false,false, cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			Assert.assertEquals(0, stored.size());
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testStoreGet() throws Exception
	{
		testStoreGet(new StorageMemory(), new StorageMemory());
		testStoreGet(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testStoreGet(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.add(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp.values(), false, false, false, cc, false);
			fr.awaitUninterruptibly();
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, null,null, false, false,false, cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			Assert.assertEquals(2, stored.size());
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testBigStorePut() throws Exception
	{
		testBigStorePut(new StorageMemory(), new StorageMemory());
		testBigStorePut(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testBigStorePut(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[100];
			byte[] me2 = new byte[10000];
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testBigStore2() throws Exception
	{
		testBigStore2(new StorageMemory(), new StorageMemory());
		testBigStore2(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	
	private void testBigStore2(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).setMaxMessageSize(Integer.MAX_VALUE).buildAndListen();
			sender.getConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).setMaxMessageSize(Integer.MAX_VALUE).buildAndListen();
			recv1.getConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[50 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			Utils.addReleaseListenerAll(fr, sender.getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testBigStoreGet() throws Exception
	{
		testBigStoreGet(new StorageMemory(), new StorageMemory());
		testBigStoreGet(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testBigStoreGet(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).setMaxMessageSize(Integer.MAX_VALUE).buildAndListen();
			sender.getConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).setMaxMessageSize(Integer.MAX_VALUE).buildAndListen();
			recv1.getConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[50 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			Utils.addReleaseListenerAll(fr, sender.getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			//
			fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			cc = fcc.getChannelCreator();
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, null,null, false,false, false, cc, false);
			Utils.addReleaseListenerAll(fr, sender.getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testBigStoreCancel() throws Exception
	{
		testBigStoreCancel(new StorageMemory(), new StorageMemory());
		testBigStoreCancel(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	
	private void testBigStoreCancel(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[50 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			Utils.addReleaseListenerAll(fr, sender.getConnectionBean().getConnectionReservation(), cc);
			Timings.sleep(100);
			fr.cancel();
			Assert.assertEquals(false, fr.isSuccess());
			System.err.println("good!");
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testBigStoreGetCancel() throws Exception
	{
		testBigStoreGetCancel(new StorageMemory(), new StorageMemory());
		testBigStoreGetCancel(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testBigStoreGetCancel(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).setMaxMessageSize(Integer.MAX_VALUE).buildAndListen();
			sender.getConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).setMaxMessageSize(Integer.MAX_VALUE).buildAndListen();
			recv1.getConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[50 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			Utils.addReleaseListener(fr, sender.getConnectionBean().getConnectionReservation(), cc, 1);
			fr.awaitUninterruptibly();
			System.err.println("XX:"+fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			//
			fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			cc = fcc.getChannelCreator();
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, null, null, false, false,false, cc, false);
			Utils.addReleaseListener(fr, sender.getConnectionBean().getConnectionReservation(), cc, 1);
			Timings.sleep(100);
			fr.cancel();
			Assert.assertEquals(false, fr.isSuccess());
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testConcurrentStoreAddGet() throws Exception
	{
		//for(int i=0;i<1000;i++)
		//{
		testConcurrentStoreAddGet(new StorageMemory(), new StorageMemory());
		testConcurrentStoreAddGet(new StorageDisk(DIR1), new StorageDisk(DIR2));
		//}
	}

	
	private void testConcurrentStoreAddGet(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			final StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			List<FutureResponse> res = new ArrayList<FutureResponse>();
			
			
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			//todo make this work...
			store(sender, recv1, smmSender, cc).awaitUninterruptibly();
			sender.getConnectionBean().getConnectionReservation().release(cc);
			for (int i = 0; i < 40; i++)
			{
				System.err.println("round "+i);
				//final ChannelCreator cc1=sender.getConnectionBean().getReservation().reserve(50);
				for (int j = 0; j < 50; j++)
				{
					FutureChannelCreator fcc1=sender.getConnectionBean().getConnectionReservation().reserve(1);
					fcc1.awaitUninterruptibly();
					ChannelCreator cc1 = fcc1.getChannelCreator();
					FutureResponse fr=store(sender, recv1, smmSender, cc1);
					res.add(fr);
					Utils.addReleaseListenerAll(fr, sender.getConnectionBean().getConnectionReservation(), cc1);
			
				}
				//cc1.release();
				for (FutureResponse fr : res)
				{
					fr.awaitUninterruptibly();
					Assert.assertEquals(true, fr.isSuccess());
				}
				res.clear();
			}
			System.err.println("done.");
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testResponsibility2() throws Exception
	{
		// Random rnd=new Random(42L);
		Peer master = null;
		Peer slave = null;
		try
		{
			master = new PeerMaker(new Number160("0xee")).buildAndListen();
			StorageGeneric s1 = new StorageMemory();
			master.getPeerBean().setStorage(s1);
			final AtomicInteger test1 = new AtomicInteger(0);
			final AtomicInteger test2 = new AtomicInteger(0);
			Replication replication=new Replication(s1, master.getPeerAddress(), master.getPeerBean().getPeerMap());
			replication.addResponsibilityListener(new ResponsibilityListener()
			{
				@Override
				public void otherResponsible(Number160 locationKey, PeerAddress other)
				{
					System.err.println("Other peer (" + other + ")is responsible for "
							+ locationKey);
					test1.incrementAndGet();
				}

				@Override
				public void meResponsible(Number160 locationKey)
				{
					System.err.println("I'm responsible for " + locationKey+" / ");
					test2.incrementAndGet();
				}
			});
			master.getPeerBean().setReplicationStorage(replication);
			Number160 location = new Number160("0xff");
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			dataMap.put(Number160.ZERO, new Data("string"));
			final FutureChannelCreator fcc=master.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = master.getStoreRPC().put(master.getPeerAddress(), location, location, dataMap, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			Utils.addReleaseListenerAll(fr, master.getConnectionBean().getConnectionReservation(), cc);
			//s1.put(location, Number160.ZERO, null, dataMap, false, false);
			slave = new PeerMaker(new Number160("0xfe")).setPorts(7701).buildAndListen();
			master.getPeerBean().getPeerMap().peerFound(slave.getPeerAddress(), null);
			master.getPeerBean().getPeerMap().peerOffline(slave.getPeerAddress(), true);
			Assert.assertEquals(1, test1.get());
			Assert.assertEquals(2, test2.get());
		}
		catch(Throwable t)
		{
			t.printStackTrace();
		}
		finally
		{
			master.shutdown();
			slave.shutdown();
		}
	}

	@Test
	public void testResponsibility3() throws Exception
	{
		Random rnd=new Random(42L);
		Peer master = null;
		Peer slave1 = null;
		Peer slave2 = null;
		try
		{
			Number160 loc=new Number160(rnd);
			Map<Number160, Data> contentMap = new HashMap<Number160, Data>();
			contentMap.put(Number160.ZERO, new Data("string"));
			final AtomicInteger test1 = new AtomicInteger(0);
			final AtomicInteger test2 = new AtomicInteger(0);
			master = new PeerMaker(new Number160(rnd)).setPorts(8000).buildAndListen();
			master.getPeerBean().getReplicationStorage().addResponsibilityListener(new ResponsibilityListener()
			{
				@Override
				public void otherResponsible(Number160 locationKey, PeerAddress other)
				{
					System.err.println("Other peer (" + other + ")is responsible for "
							+ locationKey);
					test1.incrementAndGet();
				}
				
				@Override
				public void meResponsible(Number160 locationKey)
				{
					System.err.println("I'm responsible for " + locationKey);
					test2.incrementAndGet();
				}
			});
			final FutureChannelCreator fcc=master.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			master.getStoreRPC().put(master.getPeerAddress(), loc, domainKey,  contentMap, false, false, false, cc, false).awaitUninterruptibly();
			slave1 = new PeerMaker(new Number160(rnd)).setPorts(8001).buildAndListen();
			slave2 = new PeerMaker(new Number160(rnd)).setPorts(8002).buildAndListen();
			FutureBootstrap futureBootstrap = slave1.bootstrap(master.getPeerAddress());
			futureBootstrap.awaitUninterruptibly();
			slave2.bootstrap(master.getPeerAddress()).awaitUninterruptibly();
			master.getPeerBean().getPeerMap().peerOffline(slave2.getPeerAddress(), true);
			slave2.shutdown();
			Assert.assertEquals(1, test1.get());
			Assert.assertEquals(2, test2.get());
			master.getConnectionBean().getConnectionReservation().release(cc);
		}
		catch (Throwable e)
		{
			e.printStackTrace();
		}
		finally
		{
			master.shutdown();
			slave1.shutdown();
		}
		
	}

	private FutureResponse store(Peer sender, final Peer recv1, StorageRPC smmSender, ChannelCreator cc)
			throws Exception
	{
		Map<Number160, Data> tmp = new HashMap<Number160, Data>();
		byte[] me1 = new byte[] { 1, 2, 3 };
		byte[] me2 = new byte[] { 2, 3, 4 };
		tmp.put(new Number160(77), new Data(me1));
		tmp.put(new Number160(88), new Data(me2));
		FutureResponse fr = smmSender.add(recv1.getPeerAddress(), new Number160(33),
				new ShortString("test").toNumber160(), tmp.values(), false, false, false, cc, false);
		return fr;
	}
	
	@Test
	public void testBloomFilter() throws Exception
	{
		testBloomFilter(new StorageMemory(), new StorageMemory());
		testBloomFilter(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testBloomFilter(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			byte[] me3 = new byte[] { 5, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			tmp.put(new Number160(99), new Data(me3));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			
			SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(1000, 2);
			sbf.add(new Number160(77));
			
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, sbf, null, null, false, false,false, cc, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			Assert.assertEquals(1, stored.size());
			sender.getConnectionBean().getConnectionReservation().release(cc);
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testBloomFilterDigest() throws Exception
	{
		testBloomFilterDigest(new StorageMemory(), new StorageMemory());
		testBloomFilterDigest(new StorageDisk(DIR1), new StorageDisk(DIR2));
	}

	private void testBloomFilterDigest(StorageGeneric storeSender, StorageGeneric storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			byte[] me3 = new byte[] { 5, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			tmp.put(new Number160(99), new Data(me3));
			FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false, false, cc, false);
			fr.awaitUninterruptibly();
			
			SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(1000, 2);
			sbf.add(new Number160(77));
			sbf.add(new Number160(99));
			
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, sbf, null, null, false, true,false, cc, false);
			fr.awaitUninterruptibly();
			sender.getConnectionBean().getConnectionReservation().release(cc);
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Assert.assertEquals(2, m.getKeyMap().size());
			
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
}
