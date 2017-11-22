package net.tomp2p.sctp;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.core.SctpDataCallback;
import net.sctp4nat.util.SctpUtils;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

public class SctpTest {

	static final Logger LOG = LoggerFactory.getLogger(SctpTest.class);
	int tomp2pPort = 1234;
	InetSocketAddress localAddr;

	static Random RND = new Random((new Date()).getTime());
	Number160 serverPeerId = new Number160(RND.nextInt());
	Number160 clientPeerId = new Number160(RND.nextInt());
	Peer serverPeer = null;
	Peer clientPeer = null;

	static final String TEST_STR = "Hello World!";

	@Before
	public void setUp() throws Exception {
		localAddr = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), tomp2pPort);
		SctpDataCallback cb = new SctpDataCallback() {

			@Override
			public void onSctpPacket(byte[] data, int sid, int ssn, int tsn, long ppid, int context, int flags,
					SctpChannelFacade so) {
				LOG.debug("SERVER GOT DATA: " + new String(data, StandardCharsets.UTF_8));
				assertEquals(TEST_STR, new String(data, StandardCharsets.UTF_8));
				so.send(data, 0, data.length, false, sid, (int) ppid);
				so.send(data, false, sid, (int) ppid);
			}
		};
		SctpUtils.init(localAddr.getAddress(), tomp2pPort, cb);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {

		Thread server;
		Thread client;

		CountDownLatch serverSetup = new CountDownLatch(1);

		server = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					serverPeer = new PeerBuilder(serverPeerId).udpPort(tomp2pPort).start();
				} catch (IOException e) {
					fail(e.getMessage());
				}

				if (serverPeer == null) {
					fail("no peer created");
				}

				serverSetup.countDown();
			}
		});
		server.start();

		if (!serverSetup.await(5, TimeUnit.SECONDS)) {
			fail("serverSetup timeout error");
		}

		client = new Thread(new Runnable() {

			@Override
			public void run() {

				try {
					clientPeer = new PeerBuilder(clientPeerId).udpPort(tomp2pPort + 1).start();
				} catch (IOException e) {
					fail(e.getMessage());
				}

				if (clientPeer == null) {
					fail("no peer created");
				}

				clientPeer.sendDirect(serverPeer.peerAddress()).object(TEST_STR).start();
			}
		});

		fail("Not yet implemented");
	}

}
