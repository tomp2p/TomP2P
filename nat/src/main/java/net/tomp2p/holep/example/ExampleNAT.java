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
	 * args[0] = local IP address
	 * </br>
	 * args[1] = local port
	 * </br>
	 * args[2] = isUnreachable
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 2) {
			LOG.error("Too few arguments! \n args[0] = local IP Address \n args[1] = local port \n (optional) args[2] = isUnreachable");
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
			try {
				new UnreachablePeer(new InetSocketAddress(InetAddress.getByName(args[0]), Integer.valueOf(args[1])));
			} catch (NumberFormatException e) {
				fail(e);
			} catch (UnknownHostException e) {
				fail(e);
			} catch (IOException e) {
				fail(e);
			}
		}
	}

	private static void fail(Exception e) {
		e.printStackTrace();
		LOG.error(e.getMessage());
		System.exit(1);
	}
}
