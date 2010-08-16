package net.tomp2p.rpc;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureData;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.junit.Test;

public class TestOrder
{
	@Test
	public void testOrder() throws Exception
	{
		final StringBuilder sb=new StringBuilder();
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				String s1="";
				for(int i=0;i<1000000;i++)
				{
					s1+="test"+i;
				}
				sb.append(s1);
			}
		}).start();
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			recv1.setObjectDataReply(new ObjectDataReply()
			{
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception
				{
					Integer i = (Integer) request;
					System.err.println("got "+i);
					return i + 1;
				}
			});
			for (int i = 0; i < 500; i++)
			{
				FutureData futureData = sender.send(recv1.getPeerAddress(), (Object) Integer
						.valueOf(i));
				futureData.addListener(new BaseFutureAdapter<FutureData>()
				{
					@Override
					public void operationComplete(FutureData future) throws Exception
					{
						System.err.println(future.getObject());
					}
				});
			}
			System.err.println("done");
			Utils.sleep(2000);
			
		}
		finally
		{
			sender.shutdown();
			recv1.shutdown();
		}
	}
}
