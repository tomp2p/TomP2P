/*
 * Copyright 2011 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.examples;

import java.util.Random;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureData;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class ExamplePersistentConnection 
{
	final private static Random rnd = new Random(42L);
	
	public static void main(String[] args) throws Exception 
	{
		examplePersistentConnection();
	}
	
	private static void examplePersistentConnection() throws Exception
	{
		Peer master = null;
		Peer slave = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(4001, 4001);
			//
			slave = new Peer(new Number160(rnd));
			slave.listen(4002, 4002);
			//
			slave.setObjectDataReply(new ObjectDataReply() 
			{
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception
				{
					return "world!";
				}
			});
			//keep the connection for 20s alive. Setting -1 means to keep it open as long as possible
			PeerConnection peerConnection = master.createPeerConnection(slave.getPeerAddress(), 20);
			String sentObject = "Hello";
			FutureData fd=master.send(peerConnection, sentObject);
			System.out.println("send "+ sentObject);
			fd.awaitUninterruptibly();
			System.out.println("received "+fd.getObject() + " connections: "+ChannelCreator.getStatConnectionsCreatedTCP());
			//we reuse the connection
			fd=master.send(peerConnection, sentObject);
			System.out.println("send "+ sentObject);
			fd.awaitUninterruptibly();
			System.out.println("received "+fd.getObject()  + " connections: "+ChannelCreator.getStatConnectionsCreatedTCP());
			// now we don't want to keep the connection open anymore:
			peerConnection.close();
		}
		finally
		{
			master.shutdown();
			slave.shutdown();
		}
	}
	
	
}
