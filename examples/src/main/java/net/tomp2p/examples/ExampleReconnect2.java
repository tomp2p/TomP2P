package net.tomp2p.examples;

import java.io.IOException;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

public class ExampleReconnect2 {
	public static void main(String[] args) throws IOException, InterruptedException {
		Peer A = null;
		Peer B = null;
		Peer C = null;
		Peer D = null;
		Peer E = null;
		try {
			A = new PeerBuilder(new Number160(1)).ports(7000).start();
	        B = new PeerBuilder(new Number160(2)).ports(7001).start();
	        C = new PeerBuilder(new Number160(3)).ports(7002).start();
	        D = new PeerBuilder(new Number160(4)).ports(7003).start();
	        E = new PeerBuilder(new Number160(5)).ports(7004).start();
	        
	        BaseFuture fb1 = B.bootstrap().peerAddress(A.peerAddress()).start().awaitUninterruptibly();
	        System.out.println(fb1.isSuccess() + "/" +fb1.failedReason());
	        
	        BaseFuture fb2 = C.bootstrap().peerAddress(B.peerAddress()).start().awaitUninterruptibly();
	        System.out.println(fb2.isSuccess() + "/" +fb2.failedReason());
	        
	        BaseFuture fb3 = D.bootstrap().peerAddress(C.peerAddress()).start().awaitUninterruptibly();
	        System.out.println(fb3.isSuccess() + "/" +fb3.failedReason());
	        
	        Thread.sleep(5000);
	        System.out.println("A sees: " + A.peerBean().peerMap().all());
	        System.out.println("B sees: " + B.peerBean().peerMap().all());
	        System.out.println("C sees: " + C.peerBean().peerMap().all());
	        System.out.println("D sees: " + D.peerBean().peerMap().all());
	        
	        //B goes offline
	        FutureDone<Void> fd = B.announceShutdown().start().awaitUninterruptibly();
	        B.shutdown().awaitUninterruptibly();
	        Thread.sleep(5000);
	        //comes online, bootstraps to C
	        B = new PeerBuilder(new Number160(2)).ports(7001).start();
	        BaseFuture fb4 = B.bootstrap().peerAddress(C.peerAddress()).start().awaitUninterruptibly();
	        System.out.println(fb4.isSuccess() + "/" +fb4.failedReason());
	        Thread.sleep(5000);
	        
	        System.out.println("A sees: " + A.peerBean().peerMap().all());
	        System.out.println("B sees: " + B.peerBean().peerMap().all());
	        System.out.println("C sees: " + C.peerBean().peerMap().all());
	        System.out.println("D sees: " + D.peerBean().peerMap().all());
	        
	        //E boostraps to B
	        BaseFuture fb5 = E.bootstrap().peerAddress(B.peerAddress()).start().awaitUninterruptibly();
	        System.out.println(fb5.isSuccess() + "/" +fb5.failedReason());
	        
	        Thread.sleep(5000);
	        System.out.println("A sees: " + A.peerBean().peerMap().all());
	        System.out.println("B sees: " + B.peerBean().peerMap().all());
	        System.out.println("C sees: " + C.peerBean().peerMap().all());
	        System.out.println("D sees: " + D.peerBean().peerMap().all());
	        System.out.println("E sees: " + E.peerBean().peerMap().all());
			
		} finally {
			if(A != null) {
				A.shutdown().awaitUninterruptibly();
			}
			if(B != null) {
				B.shutdown().awaitUninterruptibly();
			}
			if(C != null) {
				C.shutdown().awaitUninterruptibly();
			}
			if(D != null) {
				D.shutdown().awaitUninterruptibly();
			}
			if(E != null) {
				E.shutdown().awaitUninterruptibly();
			}
		}
	}
}
