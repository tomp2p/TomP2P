package net.tomp2p.examples;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class ExampleMultiColumn
{
	public static void main(String[] args) throws Exception
	{
		Peer master = null;
		try
		{
			Peer[] peers = ExampleUtils.createAndAttachNodes(100, 4001);
			master = peers[0];
			ExampleUtils.bootstrap(peers);
			exampleMultiColumn(peers);
		}
		finally
		{
			master.shutdown();
		}
	}

	private static <K, V> void exampleMultiColumn(Peer[] peers) throws IOException, ClassNotFoundException
	{
		// currently only contentKey splitting supported 
		String rowKey1 = "tbocek";
		String rowKey2 = "fhecht";
		String rowKey3 = "gmachado";
		String rowKey4 = "tsiaras";
		String rowKey5 = "bstiller";
		
		String col1 = "first name";
		String col2 = "last name";
		String col3 = "loaction";
		String col4 = "nr";
		
		multiAdd(peers[11], rowKey1, col1, "thomas");
		multiAdd(peers[11], rowKey1, col2, "bocek");
		multiAdd(peers[11], rowKey1, col3, "zuerich");
		multiAdd(peers[11], rowKey1, col4, "77");
		//
		multiAdd(peers[11], rowKey2, col1, "fabio");
		multiAdd(peers[11], rowKey2, col2, "hecht");
		multiAdd(peers[11], rowKey2, col3, "zuerich");
		//
		multiAdd(peers[11], rowKey3, col1, "guilherme");
		multiAdd(peers[11], rowKey3, col2, "machado");
		multiAdd(peers[11], rowKey3, col3, "zuerich");
		//
		multiAdd(peers[11], rowKey4, col1, "christos");
		multiAdd(peers[11], rowKey4, col2, "tsiaras");
		multiAdd(peers[11], rowKey4, col3, "zuerich");
		//
		multiAdd(peers[11], rowKey5, col1, "burkhard");
		multiAdd(peers[11], rowKey5, col2, "stiller");
		multiAdd(peers[11], rowKey5, col3, "zuerich");
		//
		//search
		Number160 locationKey = Number160.createHash("users");
		Number160 contentKey = combine(rowKey1, col1);
		//get entry
		FutureDHT futureDHT = peers[22].get(locationKey).setContentKey(contentKey).build();
		futureDHT.awaitUninterruptibly();
		System.out.println("single fetch: "+futureDHT.getData().getObject());
		//get list
		Set<Number160> range = new TreeSet<Number160>();
		range.add(createNr(rowKey1, 0));
		range.add(createNr(rowKey1,-1));
		futureDHT = peers[22].get(locationKey).setContentKeys(range).setRange().build();
		futureDHT.awaitUninterruptibly();
		for(Map.Entry<Number160, Data> entry: futureDHT.getDataMap().entrySet())
		{
			System.out.println("multi fetch1: "+entry.getValue().getObject());
		}
		System.out.println("col get...");
		// column get
		range = new TreeSet<Number160>();
		range.add(createNr(col1, 0));
		range.add(createNr(col1,-1));
		futureDHT = peers[22].get(locationKey).setContentKeys(range).setRange().build();
		futureDHT.awaitUninterruptibly();
		for(Map.Entry<Number160, Data> entry: futureDHT.getDataMap().entrySet())
		{
			System.out.println("multi fetch2: "+entry.getValue().getObject());
		}
	}

	private static void multiAdd(Peer peer, String rowKey, String col, String string) throws IOException
	{
		Number160 locationKey = Number160.createHash("users");
		Number160 contentKey = combine(rowKey, col);
		peer.put(locationKey).setData(contentKey, new Data(string)).build().awaitUninterruptibly();
		contentKey = combine(col, rowKey);
		peer.put(locationKey).setData(contentKey, new Data(string)).build().awaitUninterruptibly();
	}
	
	private static Number160 createNr(String key1, int nr)
	{
		Number160 firstKey = Number160.createHash(key1);
		int[] val = firstKey.toIntArray();
		val[0]=val[0] ^ val[4] ^ val[3] ^ val[2];
		val[1]=val[1] ^ val[4] ^ val[3] ^ val[2];
		val[2] = nr;
		val[3] = nr;
		val[4] = nr;
		return new Number160(val);
	}
	
	private static Number160 combine(String key1, String key2)
	{
		Number160 firstKey = Number160.createHash(key1);
		Number160 secondKey = Number160.createHash(key2);
		return combine(firstKey, secondKey);
	}
	
	private static Number160 combine(Number160 firstKey, Number160 secondKey)
	{
		int[] val1 = firstKey.toIntArray();
		val1[0]=val1[0] ^ val1[4] ^ val1[3] ^ val1[2];
		val1[1]=val1[1] ^ val1[4] ^ val1[3] ^ val1[2];
		int[] val2 = secondKey.toIntArray();
		
		val2[2]=val2[2] ^ val2[0] ^ val2[1];
		val2[3]=val2[3] ^ val2[0] ^ val2[1];
		val2[4]=val2[4] ^ val2[0] ^ val2[1];
		
		val2[0]=val1[0];
		val2[1]=val1[1];
		
		return new Number160(val2);
	}
}
