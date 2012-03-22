package net.tomp2p.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.FutureData;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.TrackerData;
import net.tomp2p.utils.Utils;

public class ExampleTracker
{
	public static void main(String[] args) throws Exception
	{
		Peer[] peers = null;
		try
		{
			peers = Examples.createAndAttachNodes(100, 4001);
			Examples.bootstrap(peers);
			MyPeer[] myPeers = wrap(peers);
			exampleTracker(myPeers);
		}
		finally
		{
			//0 is the master
			peers[0].shutdown();
		}
	}

	private static MyPeer[] wrap(Peer[] peers)
	{
		MyPeer[] retVal = new MyPeer[peers.length];
		for(int i=0;i<peers.length;i++)
		{
			retVal[i] = new MyPeer(peers[i]);
		}
		return retVal;
	}

	private static void exampleTracker(MyPeer[] peers) throws IOException, ClassNotFoundException
	{
		//3 peers have files
		System.out.println("Setup: we have "+peers.length+" peers; peers[12] has Song A, peers[24] has Song B, peers[42] has Song C");
		peers[12].announce("Song A");
		peers[24].announce("Song B");
		peers[42].announce("Song C");
		//peer 12 now searches for Song B
		System.out.println("peers[12] wants to download Song B");
		String downloaded = peers[12].download(Number160.createHash("Song B"));
		System.out.println("peers[12] got "+downloaded);
		//now peer 42 is also interested in song 24 and wants to know what other songs people listen to that downloaded song 12
		System.out.println("peers[42] wants to download Song B");
		downloaded = peers[42].download(Number160.createHash("Song B"));
		System.out.println("peers[42] got "+downloaded);
		//now peer 12 downloads again the same song 
		System.out.println("peers[12] wants to download Song B");
		downloaded = peers[12].download(Number160.createHash("Song B"));
		System.out.println("peers[12] got "+downloaded);
	}
}
class MyPeer
{
	final private Peer peer;
	final private Map<Number160, String> downloaded = new HashMap<Number160, String>();
	public MyPeer(Peer peer)
	{
		this.peer = peer;
		setReplyHandler(peer);
	}
	public void announce(String title) throws IOException
	{
		downloaded.put(Number160.createHash(title),title);
		announce();
	}
	public void announce() throws IOException
	{
		for(Map.Entry<Number160, String> entry:downloaded.entrySet())
		{
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			Collection<String> tmp = new ArrayList<String>(downloaded.values());
			tmp.remove(entry.getValue());
			cts.setAttachement(Utils.encodeJavaObject(tmp.toArray(new String[0])));
			peer.addToTracker(entry.getKey(), cts).awaitUninterruptibly();
		}
	}
	public String download(Number160 key) throws IOException, ClassNotFoundException
	{
		FutureTracker futureTracker = peer.getFromTracker(key, Configurations.defaultTrackerGetConfiguration());
		//now we know which peer has this data, and we also know what other things this peer has
		futureTracker.awaitUninterruptibly();
		Collection<TrackerData> trackerDatas = futureTracker.getTrackers();
		for(TrackerData trackerData:trackerDatas)
		{
			System.out.println("Peer "+trackerData+" claims to have the content");
			String[] attachement = (String[]) Utils.decodeJavaObject(trackerData.getAttachement(), 0, trackerData.getAttachement().length);
			for(String s1:attachement)
			{
					System.out.println("peers that downloaded this song also downloaded "+s1);
			}
		}
		System.out.println("Tracker reports that "+trackerDatas.size()+" peer(s) have this song");
		//here we download
		FutureData futureData = peer.send(trackerDatas.iterator().next().getPeerAddress(), key);
		futureData.awaitUninterruptibly();
		String downloaded = (String)futureData.getObject();
		// we need to announce that we have this piece now
		announce(downloaded);
		return downloaded;
	}
	private void setReplyHandler(Peer peer)
	{
		peer.setObjectDataReply(new ObjectDataReply()
		{
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception
			{
				if(request!=null && request instanceof Number160)
						return downloaded.get((Number160)request);
				else return null;
			}
		});
	}
}