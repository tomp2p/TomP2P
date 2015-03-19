package net.tomp2p.holep;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FutureDirect;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class HolePStressTest extends AbstractTestHoleP {

	private static final Logger LOG = LoggerFactory.getLogger(HolePStressTest.class);
	private static final int NUMBER_OF_MESSAGES = 180;
	private final AtomicInteger countDown = new AtomicInteger(NUMBER_OF_MESSAGES);
	
	@Test
	public void test() throws ClassNotFoundException, IOException {
		// set Logger Level
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
				.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.WARN);
		LOG.warn("Logger with Level " + Level.WARN.toString() + " initialized");
		
		for (int i=0; i<NUMBER_OF_MESSAGES; i++) {
			doTest(i, countDown);
//			System.out.println((i+1) + ". message sent");
		}
		
		final Long startTime = new Date().getTime();
		System.err.println("Enter Loop");
		while(countDown.get() != 0) {
			if (new Date().getTime() - startTime > 20000L) {
				break;
			}
		}
		System.err.println("Exit Loop");
	}
	
	private void doTest(final int index, final AtomicInteger countDown) throws ClassNotFoundException, IOException {
		final String requestString = "This is the test String #" + index;
		final String replyString = "SUCCESS HIT";

		unreachable2.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (requestString.equals((String) request)) {
					System.err.println("received: " + (String) request);
				}
				return replyString;
			}
		});

		FutureDirect fd = unreachable1.sendDirect(unreachable2.peerAddress()).object(requestString).forceUDP(true).start();
		fd.awaitUninterruptibly();
		if (fd.isFailed()) {
			int i = 0;
			i++;
		}
		assertEquals(true, fd.isSuccess());
		assertEquals(replyString, (String) fd.object());
		countDown.decrementAndGet();
	}

}
