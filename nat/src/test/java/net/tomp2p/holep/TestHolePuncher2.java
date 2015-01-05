package net.tomp2p.holep;

import io.netty.channel.ChannelFuture;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.RPC.Commands;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestHolePuncher2 {

	static final int IDLE_UDP_SECONDS = 30;
	static final int NUMBER_OF_HOLES = 3;
	
	// java reflection parameters
	Class noparams[] = {};
	Class[] paramInt = new Class[1];
	Class[] paramString = new Class[1];
	Class[] paramCCF = new Class[2];
	Class[] paramHPC = new Class[4];
	Class[] paramPH = new Class[1];

	Peer peer;
	Peer peer2;
	Message msg;
	static final Random RND = new Random();
	static final int PORTS = 4000;

	@Before
	public void setUp() {
		// createChannelFutures parameter
		paramCCF[0] = FutureResponse.class;
		paramCCF[1] = List.class;

		// int parameter
		paramInt[0] = Integer.TYPE;

		// HolePuncher constructor parameter
		paramHPC[0] = Peer.class;
		paramHPC[1] = Integer.TYPE;
		paramHPC[2] = Integer.TYPE;
		paramHPC[3] = Message.class;

		// prepareHandlers parameter
		paramPH[0] = FutureResponse.class;

		try {
			peer = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(
					PORTS + 1).start();
			peer2 = new PeerBuilder(Number160.createHash(RND.nextInt())).ports(
					PORTS + 1).start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		msg = new Message();
		msg.sender(peer.peerAddress());
		msg.recipient(peer2.peerAddress());
		msg.type(Type.REQUEST_2);
		msg.command(Commands.DIRECT_DATA.getNr());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCreateChannelFutures() {

		AtomicInteger atomic = new AtomicInteger(1);

		try {
			Class cls = Class.forName("net.tomp2p.holep.HolePuncher");
			Constructor constructor = cls.getConstructor(paramHPC);
			Object obj = constructor.newInstance(peer, NUMBER_OF_HOLES, IDLE_UDP_SECONDS, msg);

			Method methodCreateChannelFutures = cls.getDeclaredMethod(
					"createChannelFutures", paramCCF);
			methodCreateChannelFutures.setAccessible(true);

			Method methodPrepareHandlers = cls.getDeclaredMethod(
					"prepareHandlers", paramPH);
			methodPrepareHandlers.setAccessible(true);

			try {
				methodCreateChannelFutures.invoke(obj, null, null);
			} catch (Exception e) {
				System.err.println(atomic.decrementAndGet());
			}

			FutureDone<List<ChannelFuture>> fDone;
			try {
				FutureResponse fr = new FutureResponse(msg);

				fDone = (FutureDone<List<ChannelFuture>>) methodCreateChannelFutures.invoke(obj, fr,
						methodPrepareHandlers.invoke(obj, fr));
				
				fDone.awaitUninterruptibly();
				Assert.assertTrue(fDone.isCompleted());
				Assert.assertTrue(fDone.isSuccess());
				Assert.assertEquals(NUMBER_OF_HOLES, fDone.object().size());

			} catch (Exception e) {
				System.err.println(atomic.incrementAndGet());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		Assert.assertEquals(atomic.get(), 0);
	}
	
	@Test
	public void test
}