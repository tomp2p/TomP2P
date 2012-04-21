package net.tomp2p.examples;

import java.io.IOException;
import java.io.Serializable;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageMemory;

public class ExampleDST
{
	public static void main(String[] args) throws Exception
	{
		Peer[] peers = null;
		try
		{
			peers = ExampleUtils.createAndAttachNodes(100, 4001);
			ExampleUtils.bootstrap(peers);
			
			exampleDST(peers);
		}
		finally
		{
			//0 is the master
			peers[0].shutdown();
		}
	}

	private static void setupStorage(Peer[] peers, final int max)
	{
		for(Peer peer:peers)
		{
			StorageMemory sm = new StorageMemory()
			{
				@Override
				public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, 
						Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection)
				{
					Map<Number480, Data> map = subMap(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE);
					if(map.size()<max)
					{
						return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent, domainProtection);
					}
					else
					{
						return false;
					}
				}
				
				@Override
				public SortedMap<Number480, Data> get(Number160 locationKey, Number160 domainKey,
						Number160 fromContentKey, Number160 toContentKey)
				{
					SortedMap<Number480, Data> tmp = super.get(locationKey, domainKey, fromContentKey, toContentKey);
					return wrap(tmp);
				}

				/**
				 * We need to tell if our bag is full and if the peer should
				 * contact other peers.
				 * 
				 * @param tmp The original data
				 * @return The enriched data with a boolean flag
				 */
				private SortedMap<Number480, Data> wrap(SortedMap<Number480, Data> tmp)
				{
					SortedMap<Number480, Data> retVal = new TreeMap<Number480, Data>();
					for(Map.Entry<Number480, Data> entry:tmp.entrySet())
					{
						try
						{
							String data = (String)entry.getValue().getObject();
							retVal.put(entry.getKey(), new Data(new StringBoolean(tmp.size()<max, data)));
						}
						catch (ClassNotFoundException e)
						{
							e.printStackTrace();
						}
						catch (IOException e)
						{
							e.printStackTrace();
						}
					}
					return retVal;
				}
			};
			peer.getPeerBean().setStorage(sm);
		}
	}

	private static void exampleDST(Peer[] peers) throws IOException, ClassNotFoundException
	{
		int n=2, m=8; //1024*1024; // 2 items per peer
		setupStorage(peers, n);
		int width = m/n;
		int height = (int)log2(width);
		Interval inter = new Interval(1, m);
		putDST(peers[16], 1, "test", inter, height);
		putDST(peers[16], 3, "world", inter, height);
		putDST(peers[16], 2, "hallo", inter, height);
		putDST(peers[16], 5, "sys", inter, height);
		//search [1..5]
		Collection<Interval> inters=splitSegment(1, 5, 1, m, height);
		Collection<String> result = getDST(peers[55], inters, n);
		System.out.println("got from range query raw "+result);
		System.out.println("got from range query unique "+new HashSet<String>(result));
	}
	
	private static Collection<String> getDST(Peer peer, Collection<Interval> inters, int bagSize) throws ClassNotFoundException, IOException
	{
		Collection<String> retVal = new ArrayList<String>();
		AtomicInteger dhtCounter = new AtomicInteger();
		getDSTRec(peer, inters, retVal, new HashSet<String>(), dhtCounter, bagSize);
		System.out.println("for get we used "+dhtCounter+" DHT calls");
		return retVal;
	}
	
	private static void getDSTRec(Peer peer, Collection<Interval> inters, Collection<String> result, Collection<String> already, AtomicInteger dhtCounter, int bagSize) throws ClassNotFoundException, IOException
	{
		for(Interval inter2:inters)
		{
			//we don't query the same thing again
			if(already.contains(inter2.toString()))
				continue;
			Number160 key = Number160.createHash(inter2.toString());
			already.add(inter2.toString());
			//get the interval
			System.out.println("get for "+inter2);
			FutureDHT futureDHT = peer.getAll(key);
			futureDHT.awaitUninterruptibly();
			dhtCounter.incrementAndGet();
			for(Map.Entry<Number160, Data> entry:futureDHT.getDataMap().entrySet())
			{
				//with each result we get a flag if we should query the children (this should be returned in the future, e.g., partially_ok)
				StringBoolean stringBoolean = (StringBoolean)entry.getValue().getObject();
				result.add(stringBoolean.string);
				if(!stringBoolean.bool && inter2.size() > bagSize)
				{
					//we need to query our children
					getDSTRec(peer, inter2.split(), result, already, dhtCounter, bagSize);
				}
			}
		}
	}
	
	private static void putDST(Peer peer, int index, String word, Interval inter, int height) throws IOException
	{
		for(int i=0;i<=height;i++)
		{
			Number160 key=Number160.createHash(inter.toString());
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setContentKey(new Number160(index));
			FutureDHT futureDHT = peer.put(key, new Data(word), cs);
			futureDHT.awaitUninterruptibly();
			System.out.println("stored "+word+" in "+inter+" status: "+futureDHT.getAvgStoredKeys());
			inter = inter.split(index);
		}
		System.out.println("for DHT.put() we used "+(height+1)+" DHT calls");
	}

	private static double log2(double num)
	{
		return (Math.log(num)/Math.log(2d));
	}
	
	private static Collection<Interval> splitSegment(int s, int t, int lower, int upper, int maxDepth)
	{
		Collection<Interval> retVal = new ArrayList<Interval>();
		splitSegment(s, t, lower, upper, maxDepth, retVal);
		return retVal;
	}
	
	private static void splitSegment(int s, int t, int lower, int upper, int maxDepth, Collection<Interval> retVal)
	{
		if (s <= lower && upper <= t || maxDepth == 0)
		{
			retVal.add(new Interval(lower, upper));
			return;
		}
		int mid = (lower + upper) / 2;
		if (s<=mid)
		{
			splitSegment(s, t, lower, mid, maxDepth - 1, retVal);
		}
		if (t>mid)
		{
			splitSegment(s, t, mid+1, upper, maxDepth - 1, retVal);
		}
	}
	
	private static class StringBoolean implements Serializable
	{
		private static final long serialVersionUID = -3947493823227587011L;
		final private Boolean bool;
		final private String string;
		private StringBoolean(boolean bool, String string)
		{
			this.bool = bool;
			this.string = string;
		}
	}
	
	private static class Interval
	{
		final private int from;
		final private int to;
		
		private Interval(int from, int to)
		{
			if(from>to)
				throw new IllegalArgumentException("from cannot be greater than to");
			this.from= from;
			this.to = to;
		}
		
		@Override
		public String toString()
		{
			return "interv. ["+from+".."+to+"]";
		}
		
		Collection<Interval> split()
		{
			int mid = ((to - from) + 1) / 2;
			int tok = from + mid;
			Collection<Interval> retVal = new ArrayList<Interval>();
			retVal.add(new Interval(from, tok - 1));
			retVal.add(new Interval(tok, to));
			return retVal;
		}
		
		Interval split(int nr)
		{
			int mid = ((to - from) + 1) / 2;
			int tok = from + mid;
			if(nr<tok)
			{
				//left interval
				return new Interval(from, tok - 1);
			}
			else
			{
				//right interval
				return new Interval(tok, to);
			}
		}
		
		int size()
		{
			return (to - from) + 1;
		}
	}
}
