package net.tomp2p;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;


public class Utils2
{
	public static Message createDummyMessage() throws UnknownHostException
	{
		return createDummyMessage(false, false, false);
	}

	public static Message createDummyMessage(boolean firewallUDP, boolean firewallTCP, boolean ipv4)
			throws UnknownHostException
	{
		return createDummyMessage(new Number160("0x4321"), "127.0.0.1", 8001, 8002, new Number160(
				"0x1234"), "127.0.0.1", 8003, 8004, Command.PING, Type.REQUEST_1, firewallUDP, firewallTCP, ipv4);
	}

	public static PeerAddress createAddress(Number160 id) throws UnknownHostException
	{
		return createAddress(id, "127.0.0.1", 8005, 8006, false, false, false);
	}

	public static PeerAddress createAddress() throws UnknownHostException
	{
		return createAddress(new Number160("0x5678"), "127.0.0.1", 8005, 8006, false, false, false);
	}

	public static PeerAddress createAddress(int id) throws UnknownHostException
	{
		return createAddress(new Number160(id), "127.0.0.1", 8005, 8006, false, false, false);
	}

	public static PeerAddress createAddress(String id) throws UnknownHostException
	{
		return createAddress(new Number160(id), "127.0.0.1", 8005, 8006, false, false, false);
	}

	public static PeerAddress createAddress(Number160 idSender, String inetSender,
			int tcpPortSender, int udpPortSender, boolean firewallUDP, boolean firewallTCP, boolean ipv4)
			throws UnknownHostException
	{
		InetAddress inetSend = InetAddress.getByName(inetSender);
		PeerAddress n1 = new PeerAddress(idSender, inetSend, tcpPortSender, udpPortSender, firewallUDP, firewallTCP, ipv4);
		return n1;
	}

	public static Message createDummyMessage(Number160 idSender, String inetSender,
			int tcpPortSendor, int udpPortSender, Number160 idRecipien, String inetRecipient,
			int tcpPortRecipient, int udpPortRecipient, Command command, Type type, boolean firewallUDP, boolean firewallTCP, boolean ipv4)
			throws UnknownHostException
	{
		Message message = new Message();
		PeerAddress n1 = createAddress(idSender, inetSender, tcpPortSendor, udpPortSender,
				 firewallUDP, firewallTCP, ipv4);
		message.setSender(n1);
		//
		PeerAddress n2 = createAddress(idRecipien, inetRecipient, tcpPortRecipient,
				udpPortRecipient, firewallUDP, firewallTCP, ipv4);
		message.setRecipient(n2);
		message.setType(type);
		message.setCommand(command);
		return message;
	}
	
}
