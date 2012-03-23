package net.tomp2p.examples;

import java.io.IOException;

import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.config.ConfigurationRemove;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
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
			peers = Examples.createAndAttachNodes(100, 4001);
			Examples.bootstrap(peers);
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
		ConfigurationStore cs=Configurations.defaultStoreConfiguration();
		cs.setRefreshSeconds(2); // greater than 0
		cs.setFutureCreate(new FutureCreate<FutureDHT>()
		{
			@Override
			public void repeated(FutureDHT future)
			{
				 System.out.println("put again...");
			}
		});
		FutureDHT futureDHT=peers[1].put(Number160.ONE,  new Data("test"), cs);
		Timings.sleepUninterruptibly(9*1000);
		System.out.println("stop replication");
		futureDHT.shutdown();
		Timings.sleepUninterruptibly(9*1000);
		ConfigurationRemove cr=Configurations.defaultRemoveConfiguration();
		cr.setRefreshSeconds(2);
		cr.setRepetitions(2);
		cr.setFutureCreate(new FutureCreate<FutureDHT>()
		{
			@Override
			public void repeated(FutureDHT future)
			{
				System.out.println("remove again...");
			}
		});
		futureDHT=peers[1].remove(Number160.ONE, cr);
		Timings.sleepUninterruptibly(9*1000);
	}

}
