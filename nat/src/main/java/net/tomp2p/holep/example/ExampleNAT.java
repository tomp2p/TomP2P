package net.tomp2p.holep.example;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	 */
	public static void main(String[] args) {

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
					} else {
						new UnreachablePeer(
								new InetSocketAddress(InetAddress.getByName(args[0]), Integer.valueOf(args[1])), false,
								new InetSocketAddress(InetAddress.getByName(args[3]), Integer.valueOf(args[4])));
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
