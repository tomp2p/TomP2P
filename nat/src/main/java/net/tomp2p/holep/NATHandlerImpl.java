//package net.tomp2p.holep;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ScheduledExecutorService;
//import net.tomp2p.connection.Dispatcher;
//import net.tomp2p.connection.NATHandler;
//import net.tomp2p.futures.BaseFutureAdapter;
//import net.tomp2p.futures.FutureDone;
//import net.tomp2p.futures.FutureResponse;
//import net.tomp2p.futures.Futures;
//import net.tomp2p.holep.strategy.legacy.HolePStrategy;
//import net.tomp2p.message.Message;
//import net.tomp2p.p2p.Peer;
//import net.tomp2p.peers.PeerAddress;
//import net.tomp2p.peers.PeerSocketAddress;
//import net.tomp2p.rpc.RPC;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * @author Jonas Wagner
// *
// * This class is created if "new PeerNAT()" is called on a {@link Peer}. This class makes sure that hole
// * punching is possible.
// */
//public class NATHandlerImpl implements NATHandler {
//
//    private static final Logger LOG = LoggerFactory.getLogger(NATHandlerImpl.class);
//    private final NATTypeDetection natTypeDetection;
//    private final Peer peer;
//    private boolean testCase = false;
//    private FutureDone<NATType> future;
//
//    public NATHandlerImpl(final Peer peer) {
//        this.peer = peer;
//        this.natTypeDetection = null;
//    }
//
//    @Override
//    public FutureDone<Message> handleHolePunch(final int idleUDPMillis, final FutureResponse futureResponse,
//            final Message originalMessage) {
//        final FutureDone<Message> futureDone = new FutureDone<Message>();
//        final HolePStrategy holePuncher = natType().holePuncher(peer, 1, 30, originalMessage);
//        return holePuncher.initiateHolePunch(futureDone, futureResponse);
//    }
//
//    /**
//     * CheckNatType will trigger the {@link NATTypeDetection} object to ping the given relay peer in order to
//     * find out the {@link NATType} of this {@link Peer}.
//     *
//     * @param peerAddress
//     */
//    public FutureDone<NATType> checkNatType(final PeerAddress peerAddress) {
//        future = natTypeDetection.checkNATType(peer, peerAddress);
//        return future;
//    }
//
//    public boolean isTestCase() {
//        return testCase;
//    }
//
//    public void testCase(final boolean testCase) {
//        this.testCase = testCase;
//    }
//
////    @Override
////    public List<FutureResponse> handleRcon(Dispatcher dispatcher, Message message, FutureResponse futureResponse,
////			int idleUDPMillis, ScheduledExecutorService executorService) {
////        int i = 0;
////        dispatcher.addPendingRequest(message.messageId(), futureResponse, idleUDPMillis * 2, executorService);
////        List<FutureResponse> futures = new ArrayList<FutureResponse>(3);
////        for (PeerSocketAddress psa : peerConnection.remotePeer().relays()) {
////            Message rconMessage = createRconMessage(psa, message);
////            FutureResponse fr1 = new FutureResponse(rconMessage);
////            futures.add(fr1);
////            if (++i > 3) {
////                break;
////            }
////        }
////        Futures.<FutureResponse>whenAnySuccess(futures).addListener(
////                new BaseFutureAdapter<FutureDone<FutureResponse>>() {
////            @Override
////            public void operationComplete(FutureDone<FutureResponse> future) throws Exception {
////                if (future.isFailed()) {
////                    futureResponse.failed(future);
////                }
////            }
////        });
////        return futures;
////    }
//
//    /**
//     * This method makes a copy of the original Message and prepares it for sending it to the relay.
//     *
//     * @param message
//     * @return rconMessage
//     */
//    private static Message createRconMessage(PeerSocketAddress ps, final Message originalMessage) {
//        Message rconMessage = new Message();
//        rconMessage.sender(originalMessage.sender());
//        rconMessage.version(originalMessage.version());
//        // store the message id in the payload to get the cached message later
//        rconMessage.intValue(originalMessage.messageId());
//        // making the message ready to send
//        PeerAddress recipient = originalMessage.recipient().withIPSocket(ps).withRelaySize(0);
//        rconMessage.recipient(recipient);
//        rconMessage.command(RPC.Commands.RCON.getNr());
//        rconMessage.type(Message.Type.REQUEST_1);
//        return rconMessage;
//    }
//
//	@Override
//	public List<FutureResponse> handleRcon(Dispatcher dispatcher, Message message, FutureResponse futureResponse,
//			int idleUDPMillis, ScheduledExecutorService executorService) {
//		// TODO implement this maybe?
//		return null;
//	}
//}