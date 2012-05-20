package net.tomp2p.examples;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class ExampleConsistency
{
	final private static Random rnd = new Random(5467656537115L);
	
	public static void main(String[] args) throws Exception
	{
		Peer master = null;
		try
		{
			Peer[] peers = ExampleUtils.createAndAttachNodes(100, 4001);
			master = peers[0];
			ExampleUtils.bootstrap(peers);
			exampleConsistency(peers);
		}
		catch (Throwable e)
		{
			e.printStackTrace();
		}
		finally
		{
			master.shutdown();
		}
	}

	private static <K> void exampleConsistency(Peer[] peers) throws IOException, ClassNotFoundException
	{
		Number160 key1 = new Number160(rnd);
		System.out.println("key is "+key1);
		//find close peers
		//TreeSet<PeerAddress> set = new TreeSet<PeerAddress> (PeerMapKadImpl.createComparator(key1));
		//for(int i=0;i<peers.length;i++) System.out.println(i+ " / " +peers[i].getPeerAddress());
		//for(Peer peer:peers) set.add(peer.getPeerAddress());
		//for(PeerAddress peerAddress:set) System.out.println("close peers: "+peerAddress);
		peers[22].put(key1, new Data("Test 1")).awaitUninterruptibly();
		// close peers go offline
		peers[67].shutdown();
		peers[40].shutdown();
		peers[39].shutdown();
		peers[22].put(key1, new Data("Test 2")).awaitUninterruptibly();
		FutureDHT futureDHT = peers[33].getAll(key1);
		futureDHT.awaitUninterruptibly();
		System.out.println("got "+futureDHT.getData().getObject());
		// peer 11 and 8 joins again
		peers[67] = new PeerMaker(peers[67].getPeerID()).setMasterPeer(peers[0]).buildAndListen();
		peers[40] = new PeerMaker(peers[40].getPeerID()).setMasterPeer(peers[0]).buildAndListen();
		peers[39] = new PeerMaker(peers[39].getPeerID()).setMasterPeer(peers[0]).buildAndListen();
		peers[67].bootstrap(peers[0].getPeerAddress()).awaitUninterruptibly();
		peers[40].bootstrap(peers[0].getPeerAddress()).awaitUninterruptibly();
		peers[39].bootstrap(peers[0].getPeerAddress()).awaitUninterruptibly();
		// load old data
		peers[67].getPeerBean().getStorage().put(key1, Configurations.DEFAULT_DOMAIN, Number160.ZERO, new Data("Test 1"));
		peers[40].getPeerBean().getStorage().put(key1, Configurations.DEFAULT_DOMAIN, Number160.ZERO, new Data("Test 1"));
		peers[39].getPeerBean().getStorage().put(key1, Configurations.DEFAULT_DOMAIN, Number160.ZERO, new Data("Test 1"));
		// we got Test 1
		futureDHT = peers[0].getAll(key1);
		futureDHT.awaitUninterruptibly();
		System.out.println("peer[0] got "+futureDHT.getData().getObject());
		// we got Test 2!
		futureDHT = peers[33].getAll(key1);
		futureDHT.awaitUninterruptibly();
		System.out.println("peer[33] got "+futureDHT.getData().getObject());
		// lets attack!
		Peer mpeer1 = new PeerMaker(new Number160("0x4bca44fd09461db1981e387e99e41e7d22d06893")).setMasterPeer(peers[0]).buildAndListen();
		Peer mpeer2 = new PeerMaker(new Number160("0x4bca44fd09461db1981e387e99e41e7d22d06894")).setMasterPeer(peers[0]).buildAndListen();
		Peer mpeer3 = new PeerMaker(new Number160("0x4bca44fd09461db1981e387e99e41e7d22d06895")).setMasterPeer(peers[0]).buildAndListen();
		mpeer1.bootstrap(peers[0].getPeerAddress()).awaitUninterruptibly();
		mpeer2.bootstrap(peers[0].getPeerAddress()).awaitUninterruptibly();
		mpeer3.bootstrap(peers[0].getPeerAddress()).awaitUninterruptibly();
		// load old data
		mpeer1.getPeerBean().getStorage().put(key1, Configurations.DEFAULT_DOMAIN, Number160.ZERO, new Data("attack, attack, attack!"));
		mpeer2.getPeerBean().getStorage().put(key1, Configurations.DEFAULT_DOMAIN, Number160.ZERO, new Data("attack, attack, attack!"));
		mpeer3.getPeerBean().getStorage().put(key1, Configurations.DEFAULT_DOMAIN, Number160.ZERO, new Data("attack, attack, attack!"));
		// we got attack!
		ConfigurationGet cg = Configurations.defaultGetConfiguration();
		cg.setRequestP2PConfiguration(new RequestP2PConfiguration(6, 0, 6));
		futureDHT = peers[0].getAll(key1, cg);
		futureDHT.awaitUninterruptibly();
		System.out.println("peer[0] got "+futureDHT.getData().getObject());
		// countermeasure - statistics, pick not closest, but random peer that has the data - freshness vs. load
		for(Entry<PeerAddress, Map<Number160, Data>> entry: futureDHT.getRawData().entrySet())
		{
			System.out.print("got from "+ entry.getKey());
			System.out.println(entry.getValue());
		}
	}
}
