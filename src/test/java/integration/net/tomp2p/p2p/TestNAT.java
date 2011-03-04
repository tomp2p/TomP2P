
package net.tomp2p.p2p;

import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.Test;

public class TestNAT {
    @Test
    public void startServer() throws Exception
    {
	Random r=new Random(42L);
	Peer peer=new Peer(new Number160(r));
	peer.listen(4000, 4000);
	Thread.sleep(Integer.MAX_VALUE);
    }
    @Test
    public void startClient() throws Exception
    {
	Random r=new Random(43L);
	Peer peer=new Peer(new Number160(r));
	peer.listen(4000, 4000);
	PeerAddress pa=new PeerAddress(Number160.ZERO, InetAddress.getByName("130.60.156.176"),4000,4000);
	FutureDiscover fd=peer.discover(pa);
	fd.awaitUninterruptibly();
	if(fd.isSuccess()) {
	    System.out.println("found that my outside address is "+fd.getPeerAddress());
	}
	else {
	    System.out.println("failed "+fd.getFailedReason());
	}
    }
}