package net.tomp2p.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

public class ExampleBloomFilter
{
	final private static Random rnd = new Random(42L);

	public static void main(String[] args) throws Exception
	{
		bloomFilterBasics();
		Peer[] peers = null;
		try
		{
			peers = ExampleUtils.createAndAttachNodes(100, 4001);
			ExampleUtils.bootstrap(peers);
			exampleBloomFilter(peers);
		}
		finally
		{
			// 0 is the master
			peers[0].shutdown();
		}
	}

	private static void bloomFilterBasics()
	{
		System.out.println("bloomfilter basics:");
		SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(128, 20);
		System.out.println("false-prob. rate: "+sbf.expectedFalsePositiveProbability());
		System.out.println("init: " + sbf);
		for (int i = 0; i < 20; i++)
		{
			sbf.add(new Number160(i));
			System.out.printf("after %2d insert %s\n", (i+1), sbf);
		}
	}

	private static void exampleBloomFilter(Peer[] peers) throws IOException
	{
		Number160 nr1 = new Number160(rnd);
		Map<Number160, Data> contentMap = new HashMap<Number160, Data>();
		System.out.println("first we store 1000 items from 0-999 under key "+nr1);
		for (int i = 0; i < 1000; i++)
		{
			contentMap.put(new Number160(i), new Data("data " + i));
		}
		FutureDHT futureDHT = peers[30].put(nr1).setDataMap(contentMap).setDomainKey(Number160.createHash("my_domain")).start();
		futureDHT.awaitUninterruptibly();
		// store another one
		Number160 nr2 = new Number160(rnd);
		contentMap = new HashMap<Number160, Data>();
		System.out.println("then we store 1000 items from 800-1799 under key "+nr2);
		for (int i = 800; i < 1800; i++)
		{
			contentMap.put(new Number160(i), new Data("data " + i));
		}
		futureDHT = peers[60].put(nr2).setDataMap(contentMap).setDomainKey(Number160.createHash("my_domain")).start();
		futureDHT.awaitUninterruptibly();
		// digest the first entry
		futureDHT = peers[20].get(nr1).setDigest().setAll().setReturnBloomFilter().setDomainKey(Number160.createHash("my_domain")).start();
		futureDHT.awaitUninterruptibly();
		// we have the bloom filter for the content keys:
		SimpleBloomFilter<Number160> keyBF = futureDHT.getDigest().getKeyBloomFilter();
		System.out
				.println("We got bloomfilter for the first key. Test if bloomfilter contains 200: "
						+ keyBF.contains(new Number160(200)));
		// query for nr2, but return only those that are in this bloom filter
		futureDHT = peers[10].get(nr2).setAll().setKeyBloomFilter(keyBF).setDomainKey(Number160.createHash("my_domain")).start();
		futureDHT.awaitUninterruptibly();
		System.out.println("For the 2nd key we requested with this Bloom filer and we got "
				+ futureDHT.getDataMap().size() + " items.");
	}
}
