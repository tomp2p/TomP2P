package net.tomp2p;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;


public class Utils2
{
	public static Message createDummyMessage() throws UnknownHostException
	{
		return createDummyMessage(false, false);
	}

	public static Message createDummyMessage(boolean firewallUDP, boolean firewallTCP)
			throws UnknownHostException
	{
		return createDummyMessage(new Number160("0x4321"), "127.0.0.1", 8001, 8002, new Number160(
				"0x1234"), "127.0.0.1", 8003, 8004, Command.PING, Type.REQUEST_1, firewallUDP, firewallTCP);
	}

	public static PeerAddress createAddress(Number160 id) throws UnknownHostException
	{
		return createAddress(id, "127.0.0.1", 8005, 8006, false, false);
	}

	public static PeerAddress createAddress() throws UnknownHostException
	{
		return createAddress(new Number160("0x5678"), "127.0.0.1", 8005, 8006, false, false);
	}

	public static PeerAddress createAddress(int id) throws UnknownHostException
	{
		return createAddress(new Number160(id), "127.0.0.1", 8005, 8006, false, false);
	}

	public static PeerAddress createAddress(String id) throws UnknownHostException
	{
		return createAddress(new Number160(id), "127.0.0.1", 8005, 8006, false, false);
	}

	public static PeerAddress createAddress(Number160 idSender, String inetSender,
			int tcpPortSender, int udpPortSender, boolean firewallUDP, boolean firewallTCP)
			throws UnknownHostException
	{
		InetAddress inetSend = InetAddress.getByName(inetSender);
		PeerAddress n1 = new PeerAddress(idSender, inetSend, tcpPortSender, udpPortSender, firewallUDP, firewallTCP);
		return n1;
	}

	public static Message createDummyMessage(Number160 idSender, String inetSender,
			int tcpPortSendor, int udpPortSender, Number160 idRecipien, String inetRecipient,
			int tcpPortRecipient, int udpPortRecipient, Command command, Type type, boolean firewallUDP, boolean firewallTCP)
			throws UnknownHostException
	{
		Message message = new Message();
		PeerAddress n1 = createAddress(idSender, inetSender, tcpPortSendor, udpPortSender,
				 firewallUDP, firewallTCP);
		message.setSender(n1);
		//
		PeerAddress n2 = createAddress(idRecipien, inetRecipient, tcpPortRecipient,
				udpPortRecipient, firewallUDP, firewallTCP);
		message.setRecipient(n2);
		message.setType(type);
		message.setCommand(command);
		return message;
	}
	
	public static Peer[] createNodes(int nrOfPeers, Random rnd, int port) throws Exception 
	{
		return createNodes(nrOfPeers, rnd, port, 0, false);
	}

	/**
	 * Creates peers for testing. The first peer (peer[0]) will be used as the
	 * master. This means that shutting down peer[0] will shut down all other
	 * peers
	 * 
	 * @param nrOfPeers
	 *            The number of peers to create including the master
	 * @param rnd
	 *            The random object to create random peer IDs
	 * @param port
	 *            The port where the master peer will listen to
	 * @return All the peers, with the master peer at position 0 -> peer[0]
	 * @throws Exception
	 *             If the creation of nodes fail.
	 */
	public static Peer[] createNodes(int nrOfPeers, Random rnd, int port, int refresh, boolean enableIndirectReplication) throws Exception 
	{
		if (nrOfPeers < 1) 
		{
			throw new IllegalArgumentException("Cannot create less than 1 peer");
		}
		Peer[] peers = new Peer[nrOfPeers];
		if(refresh > 0)
		{
			if(enableIndirectReplication)
			{
				peers[0] = new PeerMaker(new Number160(rnd)).setEnableIndirectReplication(enableIndirectReplication).setTcpPort(port).setUdpPort(port).setReplicationRefreshMillis(refresh).buildAndListen();
			}
			else
			{
				peers[0] = new PeerMaker(new Number160(rnd)).setTcpPort(port).setUdpPort(port).setReplicationRefreshMillis(refresh).buildAndListen();
			}
		}
		else
		{
			if(enableIndirectReplication)
			{
				peers[0] = new PeerMaker(new Number160(rnd)).setEnableIndirectReplication(enableIndirectReplication).setTcpPort(port).setUdpPort(port).buildAndListen();
			}
			else
			{
				peers[0] = new PeerMaker(new Number160(rnd)).setTcpPort(port).setUdpPort(port).buildAndListen();
			}
		}
		for (int i = 1; i < nrOfPeers; i++) 
		{
			if(enableIndirectReplication)
			{
				peers[i] = new PeerMaker(new Number160(rnd)).setEnableIndirectReplication(enableIndirectReplication).setMasterPeer(peers[0]).buildAndListen();
			}
			else
			{
				peers[i] = new PeerMaker(new Number160(rnd)).setMasterPeer(peers[0]).buildAndListen();
			}
		}
		System.err.println("peers created.");
		return peers;
	}
	
	public static Peer[] createRealNodes(int nrOfPeers, Random rnd, int startPort)
			throws Exception {
		if (nrOfPeers < 1) {
			throw new IllegalArgumentException("Cannot create less than 1 peer");
		}
		Peer[] peers = new Peer[nrOfPeers];
		for (int i = 0; i < nrOfPeers; i++) 
		{
			peers[i] = new PeerMaker(new Number160(rnd)).setEnableMaintenance(false).setPorts(startPort + i).buildAndListen();
		}
		System.err.println("real peers created.");
		return peers;
	}

	/**
	 * Perfect routing, where each neighbor has contacted each other. This means
	 * that for small number of peers, every peer knows every other peer.
	 * 
	 * @param peers
	 *            The peers taking part in the p2p network.
	 */
	public static void perfectRouting(Peer[] peers) {
		for (int i = 0; i < peers.length; i++) {
			for (int j = 0; j < peers.length; j++)
				peers[i].getPeerBean().getPeerMap()
						.peerFound(peers[j].getPeerAddress(), null);
		}
		System.err.println("perfect routing done.");
	}
	
	public static void main(String[] args) throws IOException
	{
		createTempDirectory();
	}
	
	private static final int TEMP_DIR_ATTEMPTS = 10000;

	
	public static File createTempDirectory() throws IOException
	{
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		  String baseName = System.currentTimeMillis() + "-";

		  for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
		    File tempDir = new File(baseDir, baseName + counter);
		    if (tempDir.mkdir()) {
		      return tempDir;
		    }
		  }
		  throw new IllegalStateException("Failed to create directory within "
		      + TEMP_DIR_ATTEMPTS + " attempts (tried "
		      + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
		}

}