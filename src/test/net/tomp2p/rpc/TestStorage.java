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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.ShortString;
import net.tomp2p.replication.Replication;
import net.tomp2p.replication.ResponsibilityListener;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage;
import net.tomp2p.storage.StorageDisk;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.utils.Utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestStorage
{
	final private static Number160 domainKey = new Number160(20);
	final private static String DIR = "/tmp/blub";
	static
	{
		Handler fh;
		try
		{
			fh = new FileHandler("%t/test.log");
			fh.setFormatter(new SimpleFormatter());
			Logger.getLogger("").addHandler(fh);
			Logger.getLogger("").setLevel(Level.FINEST);
		}
		catch (SecurityException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Before
	public void befor()
	{
		new File(DIR).mkdirs();
	}

	@After
	public void after()
	{
		File f = new File(DIR);
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
	public void testStorePut() throws Exception
	{
		testStorePut(new StorageMemory(), new StorageMemory());
		testStorePut(new StorageDisk(DIR), new StorageDisk(DIR));
	}
	private void testStorePut(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			// Bindings b=new Bindings(Protocol.IPv4);
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
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
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			
			Number320 key=new Number320(new Number160(33), new ShortString("test")
			.toNumber160());
			Set<Number480> tofetch = new HashSet<Number480>();
			tofetch.add(new Number480(key, new Number160(77)));
			Map<Number160, Data> result = storeRecv.get(tofetch);
			Assert.assertEquals(result.size(), tofetch.size());
			Data c = result.get(new Number160(77));
			for (int i = 0; i < me1.length; i++)
				Assert.assertEquals(me1[i], c.getData()[i + c.getOffset()]);
			//
			tmp.clear();
			me1 = new byte[] { 5, 6, 7 };
			me2 = new byte[] { 8, 9, 1, 5 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			fr = smmSender.put(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Map<Number480, Data>  result2 = storeRecv.get(key);
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
		testStorePutIfAbsent(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testStorePutIfAbsent(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Set<Number480> tofetch = new HashSet<Number480>();
			tofetch.add(new Number480(new Number160(33), new ShortString("test")
			.toNumber160(), new Number160(77)));
			Map<Number160, Data> result = storeRecv.get(tofetch);
			Assert.assertEquals(result.size(), tofetch.size());
			Data c = result.get(new Number160(77));
			for (int i = 0; i < me1.length; i++)
				Assert.assertEquals(me1[i], c.getData()[i + c.getOffset()]);
			//
			tmp.clear();
			byte[] me3 = new byte[] { 5, 6, 7 };
			byte[] me4 = new byte[] { 8, 9, 1, 5 };
			tmp.put(new Number160(77), new Data(me3));
			tmp.put(new Number160(88), new Data(me4));
			fr = smmSender.putIfAbsent(recv1.getPeerAddress(), new Number160(33), new ShortString(
					"test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Collection<Number160> putKeys = fr.getResponse().getKeys();
			Assert.assertEquals(0, putKeys.size());
			tofetch = new HashSet<Number480>();
			tofetch.add(new Number480(new Number160(33), new ShortString("test").toNumber160(),new Number160(88)));
			result = storeRecv.get(
					tofetch);
			Assert.assertEquals(result.size(), tofetch.size());
			c = result.get(new Number160(88));
			for (int i = 0; i < me2.length; i++)
				Assert.assertEquals(me2[i], c.getData()[i + c.getOffset()]);
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
		testStorePutGet(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testStorePutGet(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp.keySet(), null, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			compare(tmp, stored);
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
		testStorePutGet2(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	
	private void testStorePutGet2(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			compare(tmp, stored);
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
		testStorePutRemoveGet(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testStorePutRemoveGet(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			// remove
			fr = smmSender.remove(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp.keySet(), true, false);
			fr.awaitUninterruptibly();
			Message m = fr.getResponse();
			Assert.assertEquals(true, fr.isSuccess());
			Map<Number160, Data> removed = m.getDataMap();
			compare(tmp, removed);
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp.keySet(), null, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			Assert.assertEquals(0, stored.size());
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
		testStorePutRemoveGet2(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testStorePutRemoveGet2(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			// remove
			fr = smmSender.remove(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), null, true, false);
			fr.awaitUninterruptibly();
			Message m = fr.getResponse();
			Assert.assertEquals(true, fr.isSuccess());
			Map<Number160, Data> removed = m.getDataMap();
			compare(tmp, removed);
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), tmp.keySet(), null, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			Assert.assertEquals(0, stored.size());
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
		testStoreGet(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testStoreGet(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[] { 1, 2, 3 };
			byte[] me2 = new byte[] { 2, 3, 4 };
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureResponse fr = smmSender.add(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp.values(), false, false);
			fr.awaitUninterruptibly();
			// get
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Message m = fr.getResponse();
			Map<Number160, Data> stored = m.getDataMap();
			Assert.assertEquals(2, stored.size());
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
		testBigStorePut(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testBigStorePut(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[100];
			byte[] me2 = new byte[10000];
			tmp.put(new Number160(77), new Data(me1));
			tmp.put(new Number160(88), new Data(me2));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
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
	public void testBigStore2() throws Exception
	{
		testBigStore2(new StorageMemory(), new StorageMemory());
		testBigStore2(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	
	private void testBigStore2(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.getConnectionConfiguration().setMaxMessageSize(Integer.MAX_VALUE);
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.getConnectionConfiguration().setMaxMessageSize(Integer.MAX_VALUE);
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[100 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
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
		testBigStoreGet(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testBigStoreGet(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.getConnectionConfiguration().setMaxMessageSize(Integer.MAX_VALUE);
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.getConnectionConfiguration().setMaxMessageSize(Integer.MAX_VALUE);
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[50 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			//
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, false);
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
		testBigStoreCancel(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	
	private void testBigStoreCancel(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[50 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			Utils.sleep(500);
			fr.cancel();
			Assert.assertEquals(false, fr.isSuccess());
			System.err.println("good!");
			//
			Utils.sleep(3000);
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
		testBigStoreGetCancel(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	private void testBigStoreGetCancel(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.getConnectionConfiguration().setMaxMessageSize(Integer.MAX_VALUE);
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.getConnectionConfiguration().setMaxMessageSize(Integer.MAX_VALUE);
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			byte[] me1 = new byte[50 * 1014 * 1024];
			tmp.put(new Number160(77), new Data(me1));
			FutureResponse fr = smmSender.put(recv1.getPeerAddress(), new Number160(33),
					new ShortString("test").toNumber160(), tmp, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			//
			fr = smmSender.get(recv1.getPeerAddress(), new Number160(33), new ShortString("test")
					.toNumber160(), null, null, false);
			Utils.sleep(500);
			fr.cancel();
			Assert.assertEquals(false, fr.isSuccess());
			//
			Utils.sleep(2000);
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
		testConcurrentStoreAddGet(new StorageMemory(), new StorageMemory());
		testConcurrentStoreAddGet(new StorageDisk(DIR), new StorageDisk(DIR));
	}

	
	private void testConcurrentStoreAddGet(Storage storeSender, Storage storeRecv) throws Exception
	{
		Peer sender = new Peer(55, new Number160("0x50"));
		Peer recv1 = new Peer(55, new Number160("0x20"));
		try
		{
			sender.listen(2424, 2424);
			recv1.listen(8088, 8088);
			sender.getPeerBean().setStorage(storeSender);
			final StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1.getPeerBean().setStorage(storeRecv);
			new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			List<FutureResponse> res = new ArrayList<FutureResponse>();
			store(sender, recv1, smmSender).awaitUninterruptibly();
			for (int i = 0; i < 1000; i++)
			{
				res.add(store(sender, recv1, smmSender));
			}
			for (int i = 0; i < 1000; i++)
			{
				res.add(get(sender, recv1, smmSender));
			}
			for (FutureResponse fr : res)
			{
				fr.awaitUninterruptibly();
				Assert.assertEquals(true, fr.isSuccess());
			}
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
			master = new Peer(new Number160("0xee"));
			master.listen();
			
			Storage s1 = new StorageMemory();
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
			master.getStoreRPC().put(master.getPeerAddress(), location, location, dataMap, false, false).awaitUninterruptibly();
			//s1.put(location, Number160.ZERO, null, dataMap, false, false);
			slave = new Peer(new Number160("0xfe"));
			slave.listen(8000, 8000);
			master.getPeerBean().getPeerMap().peerOnline(slave.getPeerAddress(), null);
			master.getPeerBean().getPeerMap().peerOffline(slave.getPeerAddress(), true);
			Assert.assertEquals(1, test1.get());
			Assert.assertEquals(2, test2.get());
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
			master = new Peer(new Number160(rnd));
			master.listen(8000,8000);
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
			master.getStoreRPC().put(master.getPeerAddress(), loc, domainKey,  contentMap, false, false).awaitUninterruptibly();
			slave1 = new Peer(new Number160(rnd));
			slave1.listen(8001,8001);
			slave2 = new Peer(new Number160(rnd));
			slave2.listen(8002,8002);
			slave1.bootstrap(master.getPeerAddress()).awaitUninterruptibly();
			slave2.bootstrap(master.getPeerAddress()).awaitUninterruptibly();
			slave2.shutdown();
			Thread.sleep(20000);
			Assert.assertEquals(1, test1.get());
			Assert.assertEquals(2, test2.get());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			master.shutdown();
			slave1.shutdown();
		}
		
	}

	private FutureResponse store(Peer sender, final Peer recv1, StorageRPC smmSender)
			throws Exception
	{
		Map<Number160, Data> tmp = new HashMap<Number160, Data>();
		byte[] me1 = new byte[] { 1, 2, 3 };
		byte[] me2 = new byte[] { 2, 3, 4 };
		tmp.put(new Number160(77), new Data(me1));
		tmp.put(new Number160(88), new Data(me2));
		FutureResponse fr = smmSender.add(recv1.getPeerAddress(), new Number160(33),
				new ShortString("test").toNumber160(), tmp.values(), false, false);
		return fr;
	}

	private FutureResponse get(Peer sender, final Peer recv1, StorageRPC smmSender)
			throws Exception
	{
		FutureResponse fr = smmSender.get(recv1.getPeerAddress(), new Number160(33),
				new ShortString("test").toNumber160(), null, null, false);
		return fr;
	}
}
