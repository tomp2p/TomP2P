package net.tomp2p.examples;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class ExampleConnect
{
	private static Peer peer1;
	private static Peer peer2;
	private static int port = 4003;

	public static void main(final String[] args) throws NumberFormatException, Exception {
		if(args.length == 0) {
			mainPeer1(args);
		}
		else {
			mainPeer2(args);
		}
	}
	
	public static void mainPeer1(final String[] args) throws NumberFormatException, Exception {
		peer1 = new Peer(new Number160(new Random())); // create a peer
		try {
			peer1.listen(port, port);
			System.out.println("We are waiting for peers...");
		} catch (Exception e) {
			System.out.println("Exception! " + e.getMessage());
			e.printStackTrace();
		}
	}

	public static void mainPeer2(final String[] args) throws NumberFormatException, Exception {
		peer2 = new Peer(new Number160(new Random())); // create a peer
		try {
			peer2.listen(port, port);
		} catch (Exception e) {
			System.out.println("Exception! " + e.getMessage());
			e.printStackTrace();
		}
		if (args.length == 3) { // Parameters: 4004 key value
			try {
				final PeerAddress pb = new PeerAddress(Number160.ZERO,
						InetAddress.getByName(args[0]), port, port);
				peer2.getP2PConfiguration().setBehindFirewall(true);
				FutureDiscover fd = peer2.discover(pb); // discover of UPNP and
				fd.addListener(new BaseFutureListener<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future)
							throws Exception {
						FutureBootstrap fb = peer2.bootstrap(pb);
						fb.addListener(new BaseFutureListener<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future)
									throws Exception {
								store(args[1], args[2]);
							}

							@Override
							public void exceptionCaught(Throwable t)
									throws Exception {
								// TODO Auto-generated method stub
								t.printStackTrace();
							}
						});
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						// TODO Auto-generated method stub
					}
				});
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void store(String name, String ip) throws IOException {
		BaseFuture future = peer2.put(Number160.createHash(name), new Data(ip));
		future.addListener(new BaseFutureAdapter<BaseFuture>() {
			@Override
			public void operationComplete(BaseFuture arg0) throws Exception {
				if (arg0.isSuccess()) {
					System.out.println("Operation success!!!");

				} else {
					System.out.println("Operation failed: "
							+ arg0.getFailedReason());
				}
			}
		});
	}
}