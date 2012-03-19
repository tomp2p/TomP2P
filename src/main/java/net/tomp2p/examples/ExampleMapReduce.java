package net.tomp2p.examples;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureLaterJoin;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.task.FutureMap;
import net.tomp2p.task.FutureMapReduce;
import net.tomp2p.task.FutureReduce;
import net.tomp2p.task.MapReducePeer;
import net.tomp2p.task.Reducer;
import net.tomp2p.task.Worker;

public class ExampleMapReduce
{
	final private static Random rnd = new Random(42L);
	
	final public static String text1 = "to be or not to be";
	final public static String text2 = "to do is to be";
	final public static String text3 = "to be is to do";
	
	public static void main(String[] args) throws Exception
	{
		MapReducePeer master = null;
		try
		{
				master = new MapReducePeer(new Number160(rnd));
				master.listen(4001, 4001);
				final MapReducePeer master2 = master;
				final FutureLateJoin<FutureMapReduce> join = new FutureLateJoin<FutureMapReduce>(3);
				join.add(map(master, text1));
				join.add(map(master, text2));
				join.add(map(master, text3));
				final FutureLaterJoin<FutureDHT> join2 = new FutureLaterJoin<FutureDHT>();
				join.addListener(new BaseFutureAdapter<FutureMapReduce>()
				{
					@Override
					public void operationComplete(FutureMapReduce future) throws Exception
					{
						//fetch the results
						for(FutureMapReduce futureMapReduce:join.getFuturesDone())
						{
							for(Number480 resultKey:futureMapReduce.resultKeys())
							{
								ConfigurationGet cg = Configurations.defaultGetConfiguration();
								cg.setDomain(resultKey.getDomainKey());
								cg.setContentKey(resultKey.getContentKey());
								join2.add(master2.get(resultKey.getLocationKey(),cg));
							}
						}
					}
				});
				join2.done();
				join2.awaitUninterruptibly();
				for(FutureDHT futureDHT:join2.getFuturesDone())
				{
					Map<Number160, Data> data = futureDHT.getData();
					for(Data data1:data.values())
					{
						Pair pair = (Pair)data1.getObject();
						System.out.println(pair.getString() + " : " + pair.getInteger());
					}
				}
		}
		finally
		{
			master.shutdown();
		}
	}

	private static FutureMapReduce map(MapReducePeer master, String text) throws IOException
	{
		FutureMapReduce futureMapReduce = master.map(Number160.createHash(text), new Data(text), new Worker()
		{
			private static final long serialVersionUID = 521373195432813612L;

			@Override
			public void map(FutureMap futureMap, MapReducePeer remotePeer, Number480 key, Storage storage) throws Exception
			{
				//this is going to be executed on a remote peer
				Data data = storage.get(key.getLocationKey(), key.getDomainKey(), key.getContentKey());
				String text = (String)data.getObject();
				String[] words = text.split(" ");  
				for (String word : words)  
				{  
					reduce(remotePeer, word);  
				}  
			}

			private FutureReduce reduce(MapReducePeer remotePeer, String word) throws IOException
			{
				FutureReduce futureReduce = remotePeer.reduce(Number160.createHash(word), new Data(1), new Reducer()
				{
					@Override
					public void reduce(Number480 key, StorageGeneric storage) throws Exception
					{
						Data dataCount = storage.get(key.getLocationKey(), key.getDomainKey(), key.getContentKey());
						Number480 resultKey = key.changeDomain(new Number160(1));
						Lock lock = storage.acquire(resultKey);
						Data dataResult = storage.get(resultKey.getLocationKey(), resultKey.getDomainKey(), resultKey.getContentKey());
						Integer count = (Integer) dataCount.getObject();
						Pair result = (Pair) dataResult.getObject();
						int sum = count + result.getInteger();
						storage.put(resultKey.getLocationKey(), resultKey.getDomainKey(), resultKey.getContentKey(), new Data(new Pair(result.getString(), sum)), null, false, false);
						storage.release(lock);
					}
				});
				return futureReduce;
			}
		});
		return futureMapReduce;
	}
	
	private static class Pair
	{
		private final String string;
		private final Integer integer;
		public Pair(String string, Integer integer)
		{
			this.string = string;
			this.integer = integer;
		}
		public Integer getInteger()
		{
			return integer;
		}
		public String getString()
		{
			return string;
		}
	}
}
