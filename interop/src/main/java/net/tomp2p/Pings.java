package net.tomp2p;

import java.io.IOException;

import org.junit.Assert;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.PingRPC;

public class Pings {
	
	private static Peer receiver = null;

	public static void startJavaPingReceiver(String argument) throws IOException, InterruptedException {
		
		// setup a receiver, write it's address to harddisk and notify via System.out

		try {
			receiver = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(7777).start();
			
			byte[] result = receiver.peerAddress().toByteArray();
			InteropUtil.writeToFile(argument, result);

			System.out.println("[---RESULT-READY---]");

		} catch (Exception ex) {
			System.err.println("Exception during startJavaPingReceiver.");
			stopJavaPingReceiver();
			throw ex;
		}
	}
	
	public static void stopJavaPingReceiver() throws InterruptedException
	{
		if (receiver != null) {
			receiver.shutdown().await();
		}
	}
	
	public static byte[] pingDotNetUdp(String argument) throws IOException, InterruptedException
	{
		// read .NET server address from harddisk
		byte[] bytes = InteropUtil.readFromFile(argument);
		PeerAddress serverAddress = new PeerAddress(bytes);
		
		// setup sender and ping .NET server
		Peer sender = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean());

            FutureChannelCreator fcc = sender.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            
            FutureResponse fr = handshake.pingUDP(serverAddress, cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            
            // check and return result of test
            boolean t1 = fr.isSuccess();
            return new byte[] { t1 ? (byte) 1 : (byte) 0 };
            
        } finally {
            if (cc != null) {
                cc.shutdown().await();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
		}
	}
}
