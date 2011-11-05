package net.tomp2p.p2p;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureData;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationRemove;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.ShortString;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage.ProtectionEnable;
import net.tomp2p.storage.Storage.ProtectionMode;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

public class TestDHT
{
	final private static Random rnd = new Random(42L);
	final private static ConnectionConfiguration CONFIGURATION = new ConnectionConfiguration();
	static
	{
		//CONFIGURATION.setIdleTCPMillis(3000000);
		//CONFIGURATION.setIdleUDPMillis(3000000);
	}
	
	@Test
	public void testTooManyOpenFilesInSystem() throws Exception
	{
		Peer master = null;
		Peer slave = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(4001, 4001);
			slave = new Peer(new Number160(rnd));
			slave.listen(4002, 4002);
			
			
			slave.setRawDataReply(new RawDataReply() 
			{	
				@Override
				public ChannelBuffer reply(PeerAddress sender, ChannelBuffer requestBuffer)
						throws Exception 
				{
					final byte[] b1=new byte[10000];
					int i=requestBuffer.getInt(0);
					ChannelBuffer ret=ChannelBuffers.wrappedBuffer(b1);
					ret.setInt(0, i);
					return ret;
				}
			});
			List<BaseFuture> list= new ArrayList<BaseFuture>();
			for(int i=0;i<100;i++)
			{
				final byte[] b=new byte[10000];
				PeerConnection pc=master.createPeerConnection(slave.getPeerAddress(), 5000);
				list.add(master.send(pc, ChannelBuffers.wrappedBuffer(b)));
			}
			for(int i=0;i<10000;i++)
			{
				list.add(master.discover(slave.getPeerAddress()));
				final byte[] b=new byte[10000];
				byte[] me=Utils.intToByteArray(i);
				System.arraycopy(me, 0, b, 0, 4);
				list.add(master.send(slave.getPeerAddress(), ChannelBuffers.wrappedBuffer(b)));
				//System.out.println(".");
			}
			for(BaseFuture bf:list)
			{
				bf.awaitUninterruptibly();
				if(bf.isFailed())
				{
					System.err.println("WTF "+bf.getFailedReason());
				}
				Assert.assertEquals(true, bf.isSuccess());
			}
			System.err.println("done!!");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			System.err.println("done!1!");
			master.shutdown();
			slave.shutdown();
		}
	}
	
	@Test
	public void testBootstrapDiscover() throws Exception
	{
		Peer master = null;
		Peer slave = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(4001, 4001);
			slave = new Peer(new Number160(rnd));
			slave.listen(4002, 4002);
			FutureDiscover fd=master.discover(slave.getPeerAddress());
			System.err.println(fd.getFailedReason());
			fd.awaitUninterruptibly();
			Assert.assertEquals(true, fd.isSuccess());
		}
		finally
		{
			master.shutdown();
			slave.shutdown();
		}
	}
	
	@Test
	public void testBootstrapFail() throws Exception
	{
		Peer master = null;
		Peer slave = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(4001, 4001);
			slave = new Peer(new Number160(rnd));
			slave.listen(4002, 4002);
			FutureBootstrap fb=master.bootstrap(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 3000));
			fb.awaitUninterruptibly();
			Assert.assertEquals(false, fb.isSuccess());
			System.err.println(fb.getFailedReason());
			fb=master.bootstrap(slave.getPeerAddress());
			fb.awaitUninterruptibly();
			Assert.assertEquals(true, fb.isSuccess());
			
		}
		finally
		{
			master.shutdown();
			slave.shutdown();
		}
	}

	@Test
	public void testBootstrap() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			// do testing
			List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
			for (int i = 0; i < peers.length; i++)
			{
				if(peers[i]!=master)
				{
					FutureBootstrap res = peers[i].bootstrap(master.getPeerAddress());
					tmp.add(res);
				}
			}
			int i = 0;
			for (FutureBootstrap fm : tmp)
			{
				fm.awaitUninterruptibly();
				if (fm.isFailed())
					System.err.println(fm.getFailedReason());
				Assert.assertEquals(true, fm.isSuccess());
				System.err.println("i:" + (++i));
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testBootstrap2() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			// do testing
			List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
			for (int i = 0; i < peers.length; i++)
			{
				if(peers[i] != master)
				{
					FutureBootstrap res = peers[i].bootstrap(master.getPeerAddress().createSocketTCP());
					tmp.add(res);
				}
			}
			int i = 0;
			for (FutureBootstrap fm : tmp)
			{
				fm.awaitUninterruptibly();
				if (fm.isFailed())
					System.err.println("FAILL:"+fm.getFailedReason());
				Assert.assertEquals(true, fm.isSuccess());
				System.err.println("i:" + (++i));
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testBootstrap3() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			// do testing
			List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
			for (int i = 0; i < peers.length; i++)
			{
				FutureBootstrap res = peers[i].bootstrapBroadcast();
				tmp.add(res);
			}
			int i = 0;
			for (FutureBootstrap fm : tmp)
			{
				fm.awaitUninterruptibly();
				if (fm.isFailed())
					System.err.println("error "+fm.getFailedReason());
				Assert.assertEquals(true, fm.isSuccess());
				System.err.println("i:" + (++i));
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testBootstrap4() throws Exception
	{
		Peer master = null;
		Peer slave = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(4001, 4001);
			slave = new Peer(new Number160(rnd));
			slave.listen(4002, 4002);
			FutureForkJoin<FutureResponse> res = slave.pingBroadcast(4001);
			res.awaitUninterruptibly();
			Assert.assertEquals(true, res.isSuccess());
		}
		finally
		{
			master.shutdown();
			slave.shutdown();
		}
	}

	@Test
	public void testPerfectRouting() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(1000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Collection<PeerAddress> pas = peers[30].getPeerBean().getPeerMap().closePeers(
					peers[30].getPeerID(), 20);
			Iterator<PeerAddress> i = pas.iterator();
			PeerAddress p1 = i.next();
			Assert.assertEquals(peers[718].getPeerAddress(), p1);
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPut() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = peers[444].put(peers[30].getPeerID(), data, cs);
			fdht.awaitUninterruptibly();
			System.err.println("Test " + fdht.getFailedReason());
			Assert.assertEquals(true, fdht.isSuccess());
			// search top 3
			TreeMap<PeerAddress, Integer> tmp = new TreeMap<PeerAddress, Integer>(peers[30]
					.getPeerBean().getPeerMap().createPeerComparator(peers[30].getPeerID()));
			int i = 0;
			for (Peer node : peers)
			{
				tmp.put(node.getPeerAddress(), i);
				i++;
			}
			Entry<PeerAddress, Integer> e = tmp.pollFirstEntry();
			System.err.println("1 (" + e.getValue() + ")" + e.getKey());
			Assert.assertEquals(peers[e.getValue()].getPeerAddress(), peers[30].getPeerAddress());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.err.println("2 (" + e.getValue() + ")" + e.getKey());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.err.println("3 " + e.getKey());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.err.println("4 " + e.getKey());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPutGetAlone() throws Exception
	{
		Peer master = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(4001, 4001);
			FutureDHT fdht = master.put(Number160.ONE, new Data("hallo"));
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			fdht = master.get(Number160.ONE);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			Data tmp = fdht.getData().get(Number160.ZERO);
			Assert.assertEquals("hallo", tmp.getObject().toString());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPut2() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(500, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(0, 0, 1);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(1, 0, 0);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = peers[444].put(peers[30].getPeerID(), data, cs);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			// search top 3
			TreeMap<PeerAddress, Integer> tmp = new TreeMap<PeerAddress, Integer>(peers[30]
					.getPeerBean().getPeerMap().createPeerComparator(peers[30].getPeerID()));
			int i = 0;
			for (Peer node : peers)
			{
				tmp.put(node.getPeerAddress(), i);
				i++;
			}
			Entry<PeerAddress, Integer> e = tmp.pollFirstEntry();
			Assert.assertEquals(peers[e.getValue()].getPeerAddress(), peers[30].getPeerAddress());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPutGet() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = peers[444].put(peers[30].getPeerID(), data, cs);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			rc = new RoutingConfiguration(0, 0, 10, 1);
			pc = new RequestP2PConfiguration(1, 0, 0);
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			cg.setDomain(new ShortString("test").toNumber160());
			cg.setContentKey(new Number160(5));
			cg.setRequestP2PConfiguration(pc);
			cg.setRoutingConfiguration(rc);
			fdht = peers[555].get(peers[30].getPeerID(), cg);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			Assert.assertEquals(1, fdht.getRawData().size());
			Assert.assertEquals(true, fdht.isMinReached());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPutGet2() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = peers[444].put(peers[30].getPeerID(), data, cs);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			cg.setDomain(new ShortString("test").toNumber160());
			cg.setContentKey(new Number160(5));
			cg.setRequestP2PConfiguration(pc);
			cg.setRoutingConfiguration(rc);
			fdht = peers[555].get(peers[30].getPeerID(), cg);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			Assert.assertEquals(3, fdht.getRawData().size());
			Assert.assertEquals(false, fdht.isMinReached());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPutGet3() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = peers[444].put(peers[30].getPeerID(), data, cs);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			rc = new RoutingConfiguration(1, 0, 10, 1);
			pc = new RequestP2PConfiguration(1, 0, 0);
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			cg.setDomain(new ShortString("test").toNumber160());
			cg.setContentKey(new Number160(5));
			cg.setRequestP2PConfiguration(pc);
			cg.setRoutingConfiguration(rc);
			for (int i = 0; i < 1000; i++)
			{
				fdht = peers[100 + i].get(peers[30].getPeerID(), cg);
				fdht.awaitUninterruptibly();
				Assert.assertEquals(true, fdht.isSuccess());
				Assert.assertEquals(1, fdht.getRawData().size());
				Assert.assertEquals(true, fdht.isMinReached());
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPutGetRemove() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = peers[444].put(peers[30].getPeerID(), data, cs);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);
			ConfigurationRemove cr = Configurations.defaultRemoveConfiguration();
			cr.setDomain(new ShortString("test").toNumber160());
			cr.setContentKey(new Number160(5));
			cr.setRequestP2PConfiguration(pc);
			cr.setRoutingConfiguration(rc);
			fdht = peers[222].remove(peers[30].getPeerID(), cr);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			Assert.assertEquals(3, fdht.getRawKeys().size());
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			cg.setDomain(new ShortString("test").toNumber160());
			cg.setContentKey(new Number160(5));
			cg.setRequestP2PConfiguration(pc);
			cg.setRoutingConfiguration(rc);
			fdht = peers[555].get(peers[30].getPeerID(), cg);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(false, fdht.isSuccess());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPutGetRemove2() throws Exception
	{
		Peer master = null;
		try
		{
			//rnd.setSeed(253406013991563L);
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = peers[444].put(peers[30].getPeerID(), data, cs);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			System.err.println("remove");
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);
			ConfigurationRemove cr = Configurations.defaultRemoveConfiguration();
			cr.setDomain(new ShortString("test").toNumber160());
			cr.setContentKey(new Number160(5));
			cr.setRequestP2PConfiguration(pc);
			cr.setRoutingConfiguration(rc);
			cr.setReturnResults(true);
			fdht = peers[222].remove(peers[30].getPeerID(), cr);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			Assert.assertEquals(3, fdht.getRawData().size());
			System.err.println("get");
			rc = new RoutingConfiguration(4, 0, 0, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			cg.setDomain(new ShortString("test").toNumber160());
			cg.setContentKey(new Number160(5));
			cg.setRequestP2PConfiguration(pc);
			cg.setRoutingConfiguration(rc);
			fdht = peers[555].get(peers[30].getPeerID(), cg);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(false, fdht.isSuccess());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testDirect() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(1000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			final AtomicInteger ai = new AtomicInteger(0);
			for (int i = 0; i < peers.length; i++)
			{
				peers[i].setObjectDataReply(new ObjectDataReply()
				{
					@Override
					public Object reply(PeerAddress sender, Object request) throws Exception
					{
						ai.incrementAndGet();
						return "ja";
					}
				});
			}
			// do testing
			FutureDHT f = peers[400].send(new Number160(rnd), "hallo");
			f.awaitUninterruptibly();
			Assert.assertEquals(true, f.isSuccess());
			Assert.assertEquals(true, ai.get() >= 3 && ai.get() <= 6);
			Assert.assertEquals("ja", f.getObject());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testAddGet() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			FutureDHT futureDHT = peers[30].add(nr, data1);
			futureDHT.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + futureDHT.isSuccess() + ")");
			futureDHT = peers[50].add(nr, data2);
			futureDHT.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + futureDHT.isSuccess() + ")");
			futureDHT = peers[77].getAll(nr);
			futureDHT.awaitUninterruptibly();
			Assert.assertEquals(true, futureDHT.isSuccess());
			Iterator<Data> iterator = futureDHT.getRawData().values().iterator().next().values()
					.iterator();
			System.out.println("got: " + new String(iterator.next().getData()) + " ("
					+ futureDHT.isSuccess() + ")");
			System.out.println("got: " + new String(iterator.next().getData()) + " ("
					+ futureDHT.isSuccess() + ")");
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testData() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			ChannelBuffer c = ChannelBuffers.dynamicBuffer();
			c.writeInt(77);
			peers[50].setRawDataReply(new RawDataReply()
			{
				@Override
				public ChannelBuffer reply(PeerAddress sender, ChannelBuffer requestBuffer)
				{
					System.err.println(requestBuffer.readInt());
					ChannelBuffer c = ChannelBuffers.dynamicBuffer();
					c.writeInt(88);
					return c;
				}
			});
			FutureData fd = master.send(peers[50].getPeerAddress(), c);
			fd.await();
			if (fd.getBuffer() == null)
				System.err.println("damm");
			int read = fd.getBuffer().readInt();
			Assert.assertEquals(88, read);
			System.err.println("done");
			//for(FutureBootstrap fb:tmp)
			//	fb.awaitUninterruptibly();
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testData2() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			ChannelBuffer c = ChannelBuffers.dynamicBuffer();
			c.writeInt(77);
			peers[50].setRawDataReply(new RawDataReply()
			{
				@Override
				public ChannelBuffer reply(PeerAddress sender, ChannelBuffer requestBuffer)
				{
					System.err.println(requestBuffer.readInt());
					return requestBuffer;
				}
			});
			FutureData fd = master.send(peers[50].getPeerAddress(), c);
			fd.await();
			System.err.println("done1");
			Assert.assertEquals(true, fd.isSuccess());
			Assert.assertNull(fd.getBuffer());
			// int read = fd.getBuffer().readInt();
			// Assert.assertEquals(88, read);
			System.err.println("done2");
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testObjectLoop() throws Exception
	{
		for (int i = 0; i < 1000; i++)
		{
			System.err.println("nr: "+i);
			testObject();
		}
	}

	@Test
	public void testObject() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			Data data1 = new Data(toStore1);
			Data data2 = new Data(toStore2);
			System.err.println("begin add : ");
			FutureDHT futureDHT = peers[30].add(nr, data1);
			futureDHT.awaitUninterruptibly();
			System.err.println("stop added: " + toStore1 + " (" + futureDHT.isSuccess() + ")");
			futureDHT = peers[50].add(nr, data2);
			futureDHT.awaitUninterruptibly();
			System.err.println("added: " + toStore2 + " (" + futureDHT.isSuccess() + ")");
			futureDHT = peers[77].getAll(nr);
			futureDHT.awaitUninterruptibly();
			if (!futureDHT.isSuccess())
				System.err.println(futureDHT.getFailedReason());
			Assert.assertEquals(true, futureDHT.isSuccess());
			
			Iterator<Data> iterator = futureDHT.getData().values().iterator();
			//futureDHT.get
			Assert.assertEquals(2, futureDHT.getData().size());
			System.err.println("got: " + iterator.next().getObject() + " ("
					+ futureDHT.isSuccess() + ")");
			System.err.println("got: " + iterator.next().getObject() + " ("
					+ futureDHT.isSuccess() + ")");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testAddGetPermits() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			List<FutureDHT> list = new ArrayList<FutureDHT>();
			for (int i = 0; i < peers.length; i++)
			{
				String toStore1 = "hallo" + i;
				Data data1 = new Data(toStore1.getBytes());
				FutureDHT futureDHT = peers[i].add(nr, data1);
				list.add(futureDHT);
			}
			for (FutureDHT futureDHT : list)
			{
				futureDHT.awaitUninterruptibly();
				Assert.assertEquals(true, futureDHT.isSuccess());
			}
			System.err.println("DONE");
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testShutdown() throws Exception
	{
		Peer master1 = null;
		Peer master2 = null;
		Peer master3 = null;
		try
		{
			ConnectionConfiguration c = new ConnectionConfiguration();
			c.setIdleTCPMillis(Integer.MAX_VALUE);
			c.setIdleUDPMillis(Integer.MAX_VALUE);
			master1 = new Peer(1, new Number160(rnd), c);
			master1.listen(4001, 4001);
			master2 = new Peer(1, new Number160(rnd), c);
			master2.listen(4002, 4002);
			master3 = new Peer(1, new Number160(rnd), c);
			master3.listen(4003, 4003);
			// perfect routing
			master1.getPeerBean().getPeerMap().peerFound(master2.getPeerAddress(), null);
			master1.getPeerBean().getPeerMap().peerFound(master3.getPeerAddress(), null);
			master2.getPeerBean().getPeerMap().peerFound(master1.getPeerAddress(), null);
			master2.getPeerBean().getPeerMap().peerFound(master3.getPeerAddress(), null);
			master3.getPeerBean().getPeerMap().peerFound(master1.getPeerAddress(), null);
			master3.getPeerBean().getPeerMap().peerFound(master2.getPeerAddress(), null);
			Number160 id = master2.getPeerID();
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(new ShortString("test").toNumber160());
			cs.setContentKey(new Number160(5));
			cs.setRequestP2PConfiguration(pc);
			cs.setRoutingConfiguration(rc);
			FutureDHT fdht = master1.put(id, data, cs);
			fdht.awaitUninterruptibly();
			Collection<Number160> tmp = new ArrayList<Number160>();
			tmp.add(new Number160(5));
			final ChannelCreator cc=master1.getConnectionBean().getReservation().reserve(1);
			
			FutureResponse fr = master1.getStoreRPC().get(master2.getPeerAddress(), id,
					new ShortString("test").toNumber160(), tmp, null, false, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(1, fr.getResponse().getDataMap().size());
			fr = master1.getStoreRPC().get(master3.getPeerAddress(), id,
					new ShortString("test").toNumber160(), tmp, null, false, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(1, fr.getResponse().getDataMap().size());
			fr = master1.getStoreRPC().get(master1.getPeerAddress(), id,
					new ShortString("test").toNumber160(), tmp, null, false, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(1, fr.getResponse().getDataMap().size());
			//
			Assert.assertEquals(true, fdht.isSuccess());
			// search top 3
			master2.shutdown();
			master2 = new Peer(new Number160(rnd));
			master2.listen(4002, 4002);
			//
			fr = master1.getStoreRPC().get(master2.getPeerAddress(), id,
					new ShortString("test").toNumber160(), tmp, null, false, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(0, fr.getResponse().getDataMap().size());
			//
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			cg.setDomain(new ShortString("test").toNumber160());
			cg.setContentKey(new Number160(5));
			cg.setRequestP2PConfiguration(pc);
			cg.setRoutingConfiguration(rc);
			fdht = master1.get(id, cg);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			System.err.println("no more exceptions here!!");
			fdht = master1.get(id, cg);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			fdht = master1.get(id, cg);
			fdht.awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
		}
		finally
		{
			master1.shutdown();
			master2.shutdown();
			master3.shutdown();
		}
	}

	@Test
	public void testLogging() throws Exception
	{
		Peer master = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(new File("/tmp/p2plog.txt.gz"));
			Peer[] peers = createNodes(master, 100);
			List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
			for (int i = 0; i < peers.length; i++)
			{
				FutureBootstrap res = peers[i].bootstrap(master.getPeerAddress().createSocketTCP());
				tmp.add(res);
			}
			int i = 0;
			for (FutureBootstrap fm : tmp)
			{
				fm.awaitUninterruptibly();
				if (fm.isFailed())
					System.err.println(fm.getFailedReason());
				Assert.assertEquals(true, fm.isSuccess());
				System.err.println("i:" + (++i));
			}
			Assert.assertEquals(true, new File("/tmp/p2plog.txt.gz").length() > 10000);
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testSecurePutGet1() throws Exception
	{
		Peer master = null;
		Peer slave1 = null;
		Peer slave2 = null;
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		KeyPair pair2 = gen.generateKeyPair();
		KeyPair pair3 = gen.generateKeyPair();
		System.err.println("PPK1 " + pair1.getPublic());
		System.err.println("PPK2 " + pair2.getPublic());
		System.err.println("PPK3 " + pair3.getPublic());
		try
		{
			master = new Peer(new Number160(rnd), pair1);
			master.listen(4001, 4001);
			master.getPeerBean().getStorage().setProtection(ProtectionEnable.ALL,
					ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
					ProtectionMode.MASTER_PUBLIC_KEY);
			slave1 = new Peer(new Number160(rnd), pair2);
			slave1.listen(master);
			slave1.getPeerBean().getStorage().setProtection(ProtectionEnable.ALL,
					ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
					ProtectionMode.MASTER_PUBLIC_KEY);
			slave2 = new Peer(new Number160(rnd), pair3);
			slave2.listen(master);
			slave2.getPeerBean().getStorage().setProtection(ProtectionEnable.ALL,
					ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
					ProtectionMode.MASTER_PUBLIC_KEY);
			// perfect routing
			master.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
			master.getPeerBean().getPeerMap().peerFound(slave2.getPeerAddress(), null);
			//
			slave1.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
			slave1.getPeerBean().getPeerMap().peerFound(slave2.getPeerAddress(), null);
			//
			slave2.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
			slave2.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
			Number160 locationKey = new Number160(50);
			ConfigurationStore cs1 = Configurations.defaultStoreConfiguration();
			Data data1 = new Data("test1");
			data1.setProtectedEntry(true);
			FutureDHT fdht1 = master.put(locationKey, data1, cs1);
			fdht1.awaitUninterruptibly();
			System.err.println(fdht1.getFailedReason());
			Assert.assertEquals(true, fdht1.isSuccess());
			// store again
			Data data2 = new Data("test1");
			data2.setProtectedEntry(true);
			FutureDHT fdht2 = slave1.put(locationKey, data2);
			fdht2.awaitUninterruptibly();
			Assert.assertEquals(0, fdht2.getKeys().size());
			Assert.assertEquals(false, fdht2.isSuccess());
			// Utils.sleep(1000000);
			// try to removze it
			FutureDHT fdht3 = slave2.remove(locationKey);
			fdht3.awaitUninterruptibly();
			// true, since we have domain protection yet
			Assert.assertEquals(true, fdht3.isSuccess());
			Assert.assertEquals(0, fdht3.getKeys().size());
			// try to put another thing
			cs1.setContentKey(new Number160(33));
			Data data3 = new Data("test2");
			data3.setProtectedEntry(true);
			FutureDHT fdht4 = master.put(locationKey, data3, cs1);
			fdht4.awaitUninterruptibly();
			Assert.assertEquals(true, fdht4.isSuccess());
			// get it
			FutureDHT fdht7 = slave2.getAll(locationKey);
			fdht7.awaitUninterruptibly();
			Assert.assertEquals(2, fdht7.getData().size());
			Assert.assertEquals(true, fdht7.isSuccess());
			//if(true)
			//	System.exit(0);
			// try to remove for real, all
			ConfigurationRemove cr = Configurations.defaultRemoveConfiguration();
			cr.setSignMessage(true);
			FutureDHT fdht5 = master.removeAll(locationKey, cr);
			fdht5.awaitUninterruptibly();
			Assert.assertEquals(true, fdht5.isSuccess());
			// get all, they should be removed now
			FutureDHT fdht6 = slave2.getAll(locationKey);
			fdht6.awaitUninterruptibly();
			Assert.assertEquals(0, fdht6.getData().size());
			Assert.assertEquals(false, fdht6.isSuccess());
			// put there the data again...
			cs1.setContentKey(Utils.makeSHAHash(pair1.getPublic().getEncoded()));
			FutureDHT fdht8 = slave1.put(locationKey, new Data("test1"), cs1);
			fdht8.awaitUninterruptibly();
			Assert.assertEquals(true, fdht8.isSuccess());
			// overwrite
			Data data4 = new Data("test1");
			data4.setProtectedEntry(true);
			FutureDHT fdht9 = master.put(locationKey, data4, cs1);
			fdht9.awaitUninterruptibly();
			System.err.println("reason " + fdht9.getFailedReason());
			Assert.assertEquals(true, fdht9.isSuccess());
		}
		finally
		{
			// Utils.sleep(1000000);
			master.shutdown();
			slave1.shutdown();
			slave2.shutdown();
		}
	}

	
	
	@Test
	public void testObjectSendExample() throws Exception
	{
		Peer p1 = null;
		Peer p2 = null;
		try
		{
			p1 = new Peer(new Number160(rnd));
			p1.listen(4001, 4001);
			p2 = new Peer(new Number160(rnd));
			p2.listen(4002, 4002);
			//attach reply handler
			p2.setObjectDataReply(new ObjectDataReply()
			{
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception
				{
					System.out.println("request ["+request+"]");
					return "world";
				}
			});
			FutureData futureData=p1.send(p2.getPeerAddress(), "hello");
			futureData.awaitUninterruptibly();
			System.out.println("reply ["+futureData.getObject()+"]");
		}
		finally
		{
			p1.shutdown();
			p2.shutdown();
		}
	}
	
	

	@Test
	public void testObjectSend() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(500, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			for (int i = 0; i < peers.length; i++)
			{
				System.err.println("node "+i);
				peers[i].setObjectDataReply(new ObjectDataReply()
				{
					@Override
					public Object reply(PeerAddress sender, Object request) throws Exception
					{
						return request;
					}
				});
				peers[i].setRawDataReply(new RawDataReply()
				{
					@Override
					public ChannelBuffer reply(PeerAddress sender, ChannelBuffer requestBuffer)
							throws Exception
					{
						return requestBuffer;
					}
				});
			}
			// do testing
			System.err.println("round start");
			Random rnd = new Random(42L);
			byte[] toStore1 = new byte[10 * 1024];
			for (int j = 0; j < 5; j++)
			{
				System.err.println("round "+j);
				for (int i = 0; i < peers.length - 1; i++)
				{
					send1(peers[rnd.nextInt(peers.length)], peers[rnd.nextInt(peers.length)],
							toStore1, 100);
					send2(peers[rnd.nextInt(peers.length)], peers[rnd.nextInt(peers.length)],
							ChannelBuffers.wrappedBuffer(toStore1), 100);
					System.err.println("round1 "+i);
				}
			}
			System.err.println("DONE");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testPassiveReplication() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
			for (int i = 0; i < peers.length; i++)
			{
				if(peers[i]!=master)
				{
					FutureBootstrap res = peers[i].bootstrap(master.getPeerAddress().createSocketTCP());
					tmp.add(res);
				}
			}
			int i = 0;
			for (FutureBootstrap fm : tmp)
			{
				fm.awaitUninterruptibly();
				if (fm.isFailed())
					System.err.println(fm.getFailedReason());
				Assert.assertEquals(true, fm.isSuccess());
				System.err.println("i:" + (++i));
			}
			final AtomicInteger counter = new AtomicInteger(0);
			final class MyStorageMemory extends StorageMemory
			{
				@Override
				public boolean put(Number480 key, Data newData, PublicKey publicKey,
						boolean putIfAbsent, boolean domainProtection)
				{
					System.err.println("here");
					counter.incrementAndGet();
					return super.put(key, newData, publicKey, putIfAbsent, domainProtection);
				}
			}
			peers[50].getPeerBean().setStorage(new MyStorageMemory());
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setRefreshSeconds(2);
			cs.setFutureCreate(new FutureCreate<FutureDHT>()
			{
				@Override
				public void repeated(FutureDHT future)
				{
					System.err.println("chain...");
				}
			});
			FutureDHT fdht = peers[1].put(peers[50].getPeerID(), new Data("test"), cs);
			Utils.sleep(9 * 1000);
			Assert.assertEquals(5, counter.get());
			fdht.cancel();
			ConfigurationRemove cr = Configurations.defaultRemoveConfiguration();
			cr.setRefreshSeconds(1);
			cr.setRepetitions(5);
			final AtomicInteger counter2 = new AtomicInteger(0);
			cr.setFutureCreate(new FutureCreate<FutureDHT>()
			{
				@Override
				public void repeated(FutureDHT future)
				{
					System.err.println("chain...");
					counter2.incrementAndGet();
				}
			});
			FutureDHT fdht2 = peers[2].remove(peers[50].getPeerID(), cr);
			Utils.sleep(9 * 1000);
			Assert.assertEquals(5, counter.get());
			Assert.assertEquals(true, fdht2.isSuccess());
			Assert.assertEquals(5, counter2.get());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testActiveReplicationForward() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			master.setDefaultStorageReplication();
			for (int i = 0; i < peers.length; i++)
				peers[i].setDefaultStorageReplication();
			Number160 locationKey = new Number160(rnd);
			// closest
			TreeSet<PeerAddress> tmp = new TreeSet<PeerAddress>(master.getPeerBean().getPeerMap()
					.createPeerComparator(locationKey));
			tmp.add(master.getPeerAddress());
			for (int i = 0; i < peers.length; i++)
				tmp.add(peers[i].getPeerAddress());
			PeerAddress closest = tmp.iterator().next();
			System.err.println("closest to " + locationKey + " is " + closest);
			// store
			Data data = new Data("Test");
			FutureDHT futureDHT = master.put(locationKey, data);
			futureDHT.awaitUninterruptibly();
			Assert.assertEquals(true, futureDHT.isSuccess());
			List<FutureBootstrap> tmp2 = new ArrayList<FutureBootstrap>();
			for (int i = 0; i < peers.length; i++)
			{
				if(peers[i]!=master)
				{
					tmp2.add(peers[i].bootstrap(master.getPeerAddress().createSocketTCP()));
				}
			}
			for (FutureBootstrap fm : tmp2)
			{
				fm.awaitUninterruptibly();
				Assert.assertEquals(true, fm.isSuccess());
			}
			for (int i = 0; i < peers.length; i++)
			{
				for (BaseFuture baseFuture : peers[i].getPendingFutures().keySet())
					baseFuture.awaitUninterruptibly();
			}
			final ChannelCreator cc=master.getConnectionBean().getReservation().reserve(1);
						
			FutureResponse futureResponse = master.getStoreRPC().get(closest, locationKey,
					Configurations.DEFAULT_DOMAIN, null, null, false, cc);
			futureResponse.awaitUninterruptibly();
			Assert.assertEquals(true, futureResponse.isSuccess());
			Assert.assertEquals(1, futureResponse.getResponse().getDataMap().size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testActiveReplicationRefresh() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			master.getP2PConfiguration().setReplicationRefreshMillis(5 * 1000);
			master.setDefaultStorageReplication();
			for (int i = 0; i < peers.length; i++)
				peers[i].setDefaultStorageReplication();
			Number160 locationKey = master.getPeerID().xor(new Number160(77));
			// store
			Data data = new Data("Test");
			FutureDHT futureDHT = master.put(locationKey, data);
			futureDHT.awaitUninterruptibly();
			Assert.assertEquals(true, futureDHT.isSuccess());
			// bootstrap
			List<FutureBootstrap> tmp2 = new ArrayList<FutureBootstrap>();
			for (int i = 0; i < peers.length; i++)
			{
				if(peers[i]!=master)
				{
					tmp2.add(peers[i].bootstrap(master.getPeerAddress().createSocketTCP()));
				}
			}
			for (FutureBootstrap fm : tmp2)
			{
				fm.awaitUninterruptibly();
				Assert.assertEquals(true, fm.isSuccess());
			}
			for (int i = 0; i < peers.length; i++)
			{
				for (BaseFuture baseFuture : peers[i].getPendingFutures().keySet())
					baseFuture.awaitUninterruptibly();
			}
			// wait for refresh
			Thread.sleep(6000);
			//
			TreeSet<PeerAddress> tmp = new TreeSet<PeerAddress>(master.getPeerBean().getPeerMap()
					.createPeerComparator(locationKey));
			tmp.add(master.getPeerAddress());
			for (int i = 0; i < peers.length; i++)
				tmp.add(peers[i].getPeerAddress());
			int i = 0;
			for (PeerAddress closest : tmp)
			{
				final ChannelCreator cc=master.getConnectionBean().getReservation().reserve(1);
				
				FutureResponse futureResponse = master.getStoreRPC().get(closest, locationKey,
						Configurations.DEFAULT_DOMAIN, null, null, false, cc);
				futureResponse.awaitUninterruptibly();
				cc.release();
				Assert.assertEquals(true, futureResponse.isSuccess());
				Assert.assertEquals(1, futureResponse.getResponse().getDataMap().size());
				i++;
				if (i >= 5)
					break;
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	// trackerStorage.setMyResponsibility(locationKey);
	private void send2(final Peer p1, final Peer p2, final ChannelBuffer toStore1, final int count)
			throws IOException
	{
		if (count == 0)
		{
			System.err.println("failed miserably");
			return;
		}
		FutureData fd = p1.send(p2.getPeerAddress(), toStore1);
		fd.addListener(new BaseFutureAdapter<FutureData>()
		{
			@Override
			public void operationComplete(FutureData future) throws Exception
			{
				if (future.isFailed())
				{
					// System.err.println(future.getFailedReason());
					send2(p1, p2, toStore1, count - 1);
				}
			}
		});
	}

	private void send1(final Peer p1, final Peer p2, final byte[] toStore1, final int count)
			throws IOException
	{
		if (count == 0)
		{
			System.err.println("failed miserably");
			return;
		}
		FutureData fd = p1.send(p2.getPeerAddress(), toStore1);
		fd.addListener(new BaseFutureAdapter<FutureData>()
		{
			@Override
			public void operationComplete(FutureData future) throws Exception
			{
				if (future.isFailed())
				{
					// System.err.println(future.getFailedReason());
					send1(p1, p2, toStore1, count - 1);
				}
			}
		});
	}

	private void testForArray(Peer peer, Number160 locationKey, boolean find)
	{
		Collection<Number160> tmp = new ArrayList<Number160>();
		tmp.add(new Number160(5));
		Number320 number320=new Number320(locationKey, new ShortString("test").toNumber160());
		Map<Number480, Data> test =peer.getPeerBean().getStorage().get(number320);
		if (find)
		{
			Assert.assertEquals(1, test.size());
			Assert.assertEquals(44444, test.get(new Number480(number320, new Number160(5))).getLength());
		}
		else
			Assert.assertEquals(0, test.size());
	}

	private Peer[] createNodes(Peer master, int nr, Random rnd) throws Exception
	{
		Peer[] nodes = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			nodes[i] = new Peer(1, new Number160(rnd), CONFIGURATION);
			nodes[i].listen(master);
			//System.err.println("go for2 "+i);
		}
		return nodes;
	}

	private Peer[] createNodes(Peer master, int nr) throws Exception
	{
		return createNodes(master, nr, rnd);
	}
	
	
}
