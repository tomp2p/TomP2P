package net.tomp2p.bitcoin;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for testing
 * @author Alexander Mülli
 *
 */
public class Utils {

    public static List<Registration> readRegistrations(File dir, String filename) {
        List<Registration> registrations = new ArrayList<Registration>();
        // read the object from file
        FileInputStream fis = null;
        ObjectInputStream in = null;
        try {
            fis = new FileInputStream(dir.getAbsoluteFile() + "/" + filename);
            in = new ObjectInputStream(fis);
            registrations = (ArrayList<Registration>) in.readObject();
            in.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return registrations;
    }

    public static Message createDummyMessage() throws UnknownHostException {
        return createDummyMessage(false, false);
    }

    public static Message createDummyMessage(boolean firewallUDP, boolean firewallTCP)
            throws UnknownHostException {
        return createDummyMessage(new Number160("0x4321"), "127.0.0.1", 8001, 8002, new Number160("0x1234"),
                "127.0.0.1", 8003, 8004, (byte) 0, Message.Type.REQUEST_1, firewallUDP, firewallTCP);
    }

    public static Message createDummyMessage(Number160 idSender, String inetSender, int tcpPortSender,
                                             int udpPortSender, Number160 idRecipient, String inetRecipient, int tcpPortRecipient,
                                             int udpPortRecipient, byte command, Message.Type type, boolean firewallUDP, boolean firewallTCP)
            throws UnknownHostException {
        Message message = new Message();
        PeerAddress n1 = createAddress(idSender, inetSender, tcpPortSender, udpPortSender, firewallUDP,
                firewallTCP);
        message.sender(n1);
        //
        PeerAddress n2 = createAddress(idRecipient, inetRecipient, tcpPortRecipient, udpPortRecipient,
                firewallUDP, firewallTCP);
        message.recipient(n2);
        message.type(type);
        message.command(command);
        return message;
    }

    public static PeerAddress createAddress(Number160 idSender, String inetSender, int tcpPortSender,
                                            int udpPortSender, boolean firewallUDP, boolean firewallTCP) throws UnknownHostException {
        InetAddress inetSend = InetAddress.getByName(inetSender);
        PeerSocketAddress peerSocketAddress = new PeerSocketAddress(inetSend, tcpPortSender, udpPortSender);
        PeerAddress n1 = new PeerAddress(idSender, peerSocketAddress, null, firewallTCP, firewallUDP, false, false, false, false,
                null, PeerAddress.EMPTY_PEER_SOCKET_ADDRESSES);
        return n1;
    }

    public static void bootstrap( PeerDHT[] peers ) {
        //make perfect bootstrap, the regular can take a while
        for(int i=0;i<peers.length;i++) {
            for(int j=0;j<peers.length;j++) {
                peers[i].peerBean().peerMap().peerFound(peers[j].peerAddress(), null, null, null);
            }
        }
    }

}
