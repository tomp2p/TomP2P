package net.tomp2p.peers;

import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.Assert;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.Test;

public class TestPeerAddress {
    @Test
    public void testPeerAddress() throws UnknownHostException {
        Number160 id = new Number160(5678);
        InetAddress address = InetAddress.getByName("127.0.0.1");
        int portTCP = 5001;
        int portUDP = 5002;
        PeerAddress pa = new PeerAddress(id, address, portTCP, portUDP);
        byte[] me = pa.toByteArray();
        PeerAddress pa2 = new PeerAddress(me);
        compare(pa, pa2);
    }

    @Test
    public void testPeerAddress2() throws UnknownHostException {
        Number160 id = new Number160("0x657435a424444522456");
        InetAddress address = InetAddress.getByName("192.168.240.230");
        int portTCP = 50301;
        int portUDP = 51002;
        PeerAddress pa = new PeerAddress(id, address, portTCP, portUDP);
        byte[] me = pa.toByteArray();
        PeerAddress pa2 = new PeerAddress(me);
        compare(pa, pa2);
    }

    @Test
    public void testPeerAddress3() throws UnknownHostException {
        PeerAddress pa1 = new PeerAddress(new Number160("0x857e35a42e444522456"),
                InetAddress.getByName("192.168.230.230"), 12345, 23456);
        PeerAddress pa2 = new PeerAddress(new Number160("0x657435a424444522456"),
                InetAddress.getByName("192.168.240.230"), 34567, 45678);

        byte[] me = new byte[200];
        int offset = 50;
        int offset2 = pa1.toByteArray(me, offset);
        pa2.toByteArray(me, offset2);
        //
        PeerAddress pa3 = new PeerAddress(me, offset);
        int offset4 = pa3.offset();
        PeerAddress pa4 = new PeerAddress(me, offset4);
        compare(pa1, pa3);
        compare(pa2, pa4);
    }

    private void compare(PeerAddress pa, PeerAddress pa2) {
        Assert.assertEquals(pa.getID(), pa2.getID());
        Assert.assertEquals(pa.createSocketTCP().getPort(), pa2.createSocketTCP().getPort());
        Assert.assertEquals(pa.createSocketTCP(), pa2.createSocketTCP());
        Assert.assertEquals(pa.createSocketUDP().getPort(), pa2.createSocketUDP().getPort());
        Assert.assertEquals(pa.createSocketUDP(), pa2.createSocketUDP());
    }
}
