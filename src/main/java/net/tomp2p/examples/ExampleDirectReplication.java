package net.tomp2p.examples;

import java.io.IOException;

import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Timings;

public class ExampleDirectReplication
{
	public static void main(String[] args) throws Exception
	{
		Peer[] peers = null;
		try
		{
			peers = ExampleUtils.createAndAttachNodes(100, 4001);
			ExampleUtils.bootstrap(peers);
			exmpleDirectReplication(peers);
		}
		finally
		{
			//0 is the master
			peers[0].shutdown();
		}
	}

	private static void exmpleDirectReplication(Peer[] peers) throws IOException
	{
		FutureCreate<FutureDHT> futureCreate1 = new FutureCreate<FutureDHT>()
		{
			@Override
			public void repeated(FutureDHT future)
			{
				 System.out.println("put again...");
			}
		};
		FutureDHT futureDHT=peers[1].put(Number160.ONE).setData(new Data("test")).setFutureCreate(futureCreate1).setRefreshSeconds(2).start();
		Timings.sleepUninterruptibly(9*1000);
		System.out.println("stop replication");
		futureDHT.shutdown();
		Timings.sleepUninterruptibly(9*1000);
		FutureCreate<FutureDHT> futureCreate2 = new FutureCreate<FutureDHT>()
		{
			@Override
			public void repeated(FutureDHT future)
			{
				System.out.println("remove again...");
			}
		};
		futureDHT=peers[1].remove(Number160.ONE).setFutureCreate(futureCreate2).setRefreshSeconds(2).setRepetitions(2).start();
		Timings.sleepUninterruptibly(9*1000);
	}

}
