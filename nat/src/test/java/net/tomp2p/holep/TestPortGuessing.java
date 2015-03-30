package net.tomp2p.holep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.holep.strategy.NonPreservingSequentialStrategy;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.RPC;
import net.tomp2p.utils.Utils;

import org.junit.Test;

// TODO jwa --> maybe delete this class

@SuppressWarnings("rawtypes")
public class TestPortGuessing extends AbstractTestHoleP {

	private Class[] nonPreservingSeqConstructorParam = new Class[4];
	private Class[] paramInitiatingPeer = new Class[3];
	private Class[] paramTargetPeer = new Class[2];

	@SuppressWarnings("unchecked")
	@Test
	public void testHolePunchNonPreservingSequential() {

		Message originalMessage = localSetup();

		final int startingPort = 1024;
		final List mockedList = mock(List.class);
		final ChannelFuture mockedChannelFuture = mock(ChannelFuture.class);
		final Channel mockedChannel = mock(Channel.class);
		for (int i = 0; i < unreachable1.peerBean().holePNumberOfHoles(); i++) {
			when(mockedList.get(i)).thenReturn(mockedChannelFuture);
		}
		when(mockedList.size()).thenReturn(unreachable1.peerBean().holePNumberOfHoles());
		when(mockedChannelFuture.channel()).thenReturn(mockedChannel);
		when(mockedChannel.localAddress()).thenReturn(new InetSocketAddress(1337));

		List<Integer> portList = null;
		try {
			Class cls = Class.forName("net.tomp2p.holep.strategy.NonPreservingSequentialStrategy");
			Constructor constructor = null;
			constructor = cls.getConstructor(nonPreservingSeqConstructorParam);
			constructor.setAccessible(true);
			Object nonPreservingSeqObj = constructor.newInstance(unreachable1, unreachable1.peerBean().holePNumberOfHoles(), IDLE_UDP_SECONDS, originalMessage);

			Method guessPortsInitiatingPeer = cls.getDeclaredMethod("guessPortsInitiatingPeer", paramInitiatingPeer);
			guessPortsInitiatingPeer.setAccessible(true);
			guessPortsInitiatingPeer.invoke(nonPreservingSeqObj, originalMessage, mockedList, startingPort);

			portList = (List<Integer>) Utils.decodeJavaObject(originalMessage.buffer(0).buffer());

			assertEquals(unreachable1.peerBean().holePNumberOfHoles(), originalMessage.intAt(0).intValue());
			assertEquals(portList.size(), originalMessage.intAt(0).intValue());
			for (int i = 0; i < portList.size(); i++) {
				assertEquals(portList.get(i).intValue(), startingPort + 2*i);
			}
		} catch (Exception e) {
			handleFail(e);
		}
		
		List mockedList2 = mock(List.class);
		try {
			originalMessage = localSetup();
			byte[] bytes = Utils.encodeJavaObject(portList);
			Buffer byteBuf = new Buffer(Unpooled.wrappedBuffer(bytes));
			originalMessage.buffer(byteBuf);
			
			Class cls = Class.forName("net.tomp2p.holep.strategy.NonPreservingSequentialStrategy");
			Constructor constructor = null;
			constructor = cls.getConstructor(nonPreservingSeqConstructorParam);
			constructor.setAccessible(true);
			Object nonPreservingSeqObj = constructor.newInstance(unreachable1, unreachable1.peerBean().holePNumberOfHoles(), IDLE_UDP_SECONDS, originalMessage);
			
			NonPreservingSequentialStrategy nonPreservingSequentialStrategy = new NonPreservingSequentialStrategy(unreachable1, unreachable1.peerBean().holePNumberOfHoles(), IDLE_UDP_SECONDS, originalMessage);
			Class class2 = nonPreservingSequentialStrategy.getClass().getSuperclass();
			Field channelF = class2.getDeclaredField("channelFutures");
			channelF.setAccessible(true);
			channelF.set(class2, (List) new ArrayList<Integer>());
			
			Class abstractHolePClass = nonPreservingSeqObj.getClass().getSuperclass();
			Field channelFutures = abstractHolePClass.getDeclaredField("channelFutures");
			channelFutures.setAccessible(true);
			Object abstractHolePInstance = abstractHolePClass.newInstance();
			channelFutures.set(abstractHolePInstance, mockedList2);
			
			Method guessPortsTargetPeer = cls.getDeclaredMethod("guessPortsTargetPeer", paramTargetPeer);
			guessPortsTargetPeer.setAccessible(true);
			guessPortsTargetPeer.invoke(nonPreservingSeqObj, originalMessage, startingPort);
//			
		} catch (Exception e) {
			handleFail(e);
		}
	
	}
	
	private Message localSetup() {
		// HolePuncher constructor parameter
		nonPreservingSeqConstructorParam[0] = Peer.class;
		nonPreservingSeqConstructorParam[1] = Integer.TYPE;
		nonPreservingSeqConstructorParam[2] = Integer.TYPE;
		nonPreservingSeqConstructorParam[3] = Message.class;

		paramInitiatingPeer[0] = Message.class;
		paramInitiatingPeer[1] = List.class;
		paramInitiatingPeer[2] = Integer.TYPE;
		
		paramTargetPeer[0] = Message.class;
		paramTargetPeer[1] = Integer.TYPE;

		Message originalMessage = new Message();
		originalMessage.sender(unreachable1.peerAddress());
		originalMessage.recipient(unreachable2.peerAddress());
		originalMessage.type(Message.Type.REQUEST_1);
		originalMessage.command(RPC.Commands.HOLEP.getNr());
		return originalMessage;
	}

	private void handleFail(Exception e) {
		e.printStackTrace();
		fail(e.getMessage());
	}
}
