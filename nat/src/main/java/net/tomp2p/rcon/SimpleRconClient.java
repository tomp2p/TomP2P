package net.tomp2p.rcon;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class SimpleRconClient {

	private static int port = 4001;
	private static Peer peer;
	private static PeerAddress master;
	private static String ipAddress;

	public static void start() {
		// Create a peer with a random peerID, on port 4001, listening to the
		// interface eth0
		try {
			int rand = RandomUtil.getNext();
			System.out.println("RandomUtil: " + rand);
			peer = new PeerBuilder(new Number160(RandomUtil.getNext())).ports(
					port).start();

			peer.objectDataReply(new ObjectDataReply() {

				@Override
				public Object reply(PeerAddress sender, Object request)
						throws Exception {
					System.out.println("HITHITHITHITHITHITHITHIT");
					
					String req = (String) request;
					System.out.println(req);
					
					String reply = "reply";
					return (Object) reply;
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(peer.peerAddress().toString());
	}

	public static Peer getPeer() {
		return peer;
	}

	public static boolean usualBootstrap(String ip) {
		boolean success = false;
		ipAddress = ip;

		master = createPeerAddress(ipAddress);
		
		// do PeerDiscover
		FutureDiscover fd = peer.discover().peerAddress(peer.peerAddress())
				.start().awaitUninterruptibly();
		if (!fd.isSuccess()) {
			return success;
		}

		FutureBootstrap fb = peer.bootstrap()
				.peerAddress(master).start();
		fb.awaitUninterruptibly();
		if (fb.isSuccess()) {
			System.out.println("Bootstrap success!");
			success = true;
		} else {
			System.out.println("Bootstrap fail!");
		}

		return success;
	}
	
	public static boolean sendDummy(String dummy, boolean nat) {
		boolean success = false;
		
//		if (nat == true) {
			try {
				master = new PeerAddress(Number160.ZERO, InetAddress.getByName(ipAddress), port, port);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
//		}
		
		FutureDirect fd = peer.sendDirect(master).object(dummy).start();
		fd.awaitUninterruptibly();

		if(fd.isSuccess()) {
			System.out.println("FUTURE DIRECT SUCCESS!");
			success = true;
		} else {
			
		}
		
		return success;
	}
	
	/*
     * Creates peer address
     */
    private static PeerAddress createPeerAddress(String ip) {

        // Format IP
        InetAddress address = null;
        try {
            address = Inet4Address.getByName(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // Create PeerAddress for MasterNode
        PeerAddress peerAddress = null;
        FutureDiscover fd = peer.discover().inetAddress(address).ports(port)
                .start();
        fd.awaitUninterruptibly();
        if (fd.isSuccess()) {
            peerAddress = fd.peerAddress();
        } else {
            System.out.println("Discover is not working");
        }

        if (peerAddress == null) {
            System.out.println("PeerAddress fail");
        } else {
            if (peerAddress.peerId() == null) {
                System.out.println("PeerAddress ID is zero");
            } else {
                System.out.println("Create PeerAddress: " + peerAddress.toString());
            }
        }

        return peerAddress;
    }

	public static void natBootstrap(String ip) {
		try {
			peer.shutdown();
			peer = new PeerBuilder(new Number160(RandomUtil.getNext())).behindFirewall(true).ports(port).start();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		PeerNAT peerNAT = new PeerNAT(peer);
		PeerAddress pa = null;
		try {
			pa = new PeerAddress(Number160.ZERO, InetAddress.getByName(ip), port, port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		if (peerNAT.bootstrapBuilder() == null) {
			System.out.println();
			System.out.println("BOOTSTRAPBUILDER IS STILL NULL");
			System.out.println();
			peerNAT.bootstrapBuilder(peer.bootstrap().peerAddress(pa));
		}
		
		//Check if peer is reachable from the internet
		FutureDiscover fd = peer.discover().peerAddress(pa).start();
		// Try to set up port forwarding with UPNP and NATPMP if peer is not reachable 
		FutureNAT fn = peerNAT.startSetupPortforwarding(fd);
		//if port forwarding failed, this will set up relay peers
		FutureRelayNAT frn = peerNAT.startRelay(fn);
		fd.awaitUninterruptibly();
		frn.awaitUninterruptibly();
		//now the peer should be reachable
		
	}
}
