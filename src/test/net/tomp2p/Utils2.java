package net.tomp2p;
import java.net.InetAddress;
import java.net.UnknownHostException;

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

	public static Message createDummyMessage(boolean forward, boolean firewallUDP, boolean firewallTCP)
			throws UnknownHostException
	{
		return createDummyMessage(new Number160("0x4321"), "127.0.0.1", 8001, 8002, new Number160(
				"0x1234"), "127.0.0.1", 8003, 8004, Command.PING, Type.REQUEST_1, forward, firewallUDP, firewallTCP);
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
			int tcpPortSender, int udpPortSender, boolean forward, boolean firewallUDP, boolean firewallTCP)
			throws UnknownHostException
	{
		InetAddress inetSend = InetAddress.getByName(inetSender);
		PeerAddress n1 = new PeerAddress(idSender, inetSend, tcpPortSender, udpPortSender,
				forward, firewallUDP, firewallTCP);
		return n1;
	}

	public static Message createDummyMessage(Number160 idSender, String inetSender,
			int tcpPortSendor, int udpPortSender, Number160 idRecipien, String inetRecipient,
			int tcpPortRecipient, int udpPortRecipient, Command command, Type type, boolean forward, boolean firewallUDP, boolean firewallTCP)
			throws UnknownHostException
	{
		Message message = new Message();
		PeerAddress n1 = createAddress(idSender, inetSender, tcpPortSendor, udpPortSender,
				forward, firewallUDP, firewallTCP);
		message.setSender(n1);
		//
		PeerAddress n2 = createAddress(idRecipien, inetRecipient, tcpPortRecipient,
				udpPortRecipient, forward, firewallUDP, firewallTCP);
		message.setRecipient(n2);
		message.setType(type);
		message.setCommand(command);
		return message;
	}

}
