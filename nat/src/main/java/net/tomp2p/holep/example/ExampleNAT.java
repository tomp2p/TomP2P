package net.tomp2p.holep.example;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.util.SctpInitException;

/**
 * 
 * Example class to simulate NAT behaviour with TomP2P peers.
 * 
 * @author Jonas Wagner
 *
 */
public class ExampleNAT {

	private static final Logger LOG = LoggerFactory.getLogger(ExampleNAT.class);

	/**
	 * args[0] = local IP address </br>
	 * args[1] = local port </br>
	 * args[2] = isUnreachable && (if u1 -> unreachable 1 else unreachable 2)
	 * 
	 * @param args
	 * @throws SctpInitException 
	 */
	public static void main(String[] args) throws SctpInitException {

		if (args.length < 2) {
			LOG.error(
					"Too few arguments! \n args[0] = local IP Address \n args[1] = local port \n (optional) args[2] = isUnreachable");
			System.exit(1);
		} else if (args.length == 2) {
			/**
			 * This is a relay peer
			 */
			LOG.warn("starting peer in relaying mode");
			try {
				new RelayPeer(new InetSocketAddress(InetAddress.getByName(args[0]), Integer.valueOf(args[1])));
			} catch (NumberFormatException e) {
				fail(e);
			} catch (UnknownHostException e) {
				fail(e);
			} catch (IOException e) {
				fail(e);
			}
		} else {
			/**
			 * This is an unreachable peer
			 */
			if (args.length >= 5) {
				try {

					if (args[2].equals("u1")) {
						new UnreachablePeer(
								new InetSocketAddress(InetAddress.getByName(args[0]), Integer.valueOf(args[1])), true,
								new InetSocketAddress(InetAddress.getByName(args[3]), Integer.valueOf(args[4])));
					} else if (args[2].equals("u2")) {
						new UnreachablePeer(
								new InetSocketAddress(InetAddress.getByName(args[0]), Integer.valueOf(args[1])), false,
								new InetSocketAddress(InetAddress.getByName(args[3]), Integer.valueOf(args[4])));
					} else if (args[2].equals("manualPunch")) {
						boolean connected = false;
						Thread manualPunch = new Thread(new Runnable() {

							@Override
							public void run() {
								while (!connected) {
									LOG.debug("punch a hole manually with local {} and remote {}",
											args[0] + ":" + args[1], args[2] + ":" + args[4]);
									try {
										while (!connected) {
										LOG.error("entering manual hole punching thread...");
										HoleCheater.cheatHolePunch(args[0], Integer.valueOf(args[1]), args[3],
												Integer.valueOf(args[4]));

										Thread.sleep(10000); // refresh NAT mapping after 20 seconds
										}
										LOG.error("exiting manual hole punching thread...");
									} catch (NumberFormatException | IOException | InterruptedException e) {
										fail(e);
									}
								}
							}
						});

						new UnreachablePeer(
								new InetSocketAddress(InetAddress.getByName(args[0]), Integer.valueOf(args[1])),
								new InetSocketAddress(InetAddress.getByName(args[3]), Integer.valueOf(args[4])),
								connected, manualPunch, args[5].equals("u1") ? true : false);
					}
				} catch (NumberFormatException e) {
					fail(e);
				} catch (UnknownHostException e) {
					fail(e);
				} catch (IOException e) {
					fail(e);
				}
			} else {
				LOG.error("too few arguments for unreachable peer");
				System.exit(1);
			}
		}
	}

	private static void fail(Exception e) {
		e.printStackTrace();
		LOG.error(e.getMessage());
		System.exit(1);
	}
}
