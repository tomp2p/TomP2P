package net.tomp2p.relay;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);
    private final ConnectionConfiguration config;
    private final Peer peer;

    /**
     * Register the RelayRPC. After the setup, the peer is ready to act as a
     * relay if asked by an unreachable peer.
     * 
     * @param peer
     *            The peer to register the RelayRPC
     * @return
     */
    public RelayRPC(Peer peer) {
        super(peer.peerBean(), peer.connectionBean());
        register(RPC.Commands.RELAY.getNr());
        this.peer = peer;
        config = new DefaultConnectionConfiguration();
    }

    /**
     * Send the peer map of an unreachable peer to a relay peer, so that the
     * relay peer can reply to neighbor requests on behalf of the unreachable
     * peer.
     * 
     * @param peerAddress
     *            The peer address of the relay peer
     * @param map
     *            The unreachable peer's peer map.
     * @param fcc
     * @return
     */
    public FutureResponse sendPeerMap(PeerAddress peerAddress, List<Map<Number160, PeerStatatistic>> map, final PeerConnection peerConnection) {
        final Message message = createMessage(peerAddress, RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
        message.keepAlive(true);
        // TODO: neighbor size limit is 256, we might have more here
        message.neighborsSet(new NeighborSet(-1, RelayUtils.flatten(map)));
        final FutureResponse futureResponse = new FutureResponse(message);
        return sendSingle(peerConnection, futureResponse);
    }

    /**
     * Forward a message through the open peer connection to the unreachable
     * peer.
     * 
     * @param peerConnection
     *            The open connection to the unreachable peer
     * @param buf
     *            Buffer of the message that needs to be forwarded to the
     *            unreachable peer
     * @return
     */
    public FutureResponse forwardMessage(final PeerConnection peerConnection, final Buffer buf) {
        final Message message = createMessage(peerConnection.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_2);
        message.keepAlive(true);
        message.buffer(buf);
        final FutureResponse futureResponse = new FutureResponse(message);
        return sendSingle(peerConnection, futureResponse);
    }

	private FutureResponse sendSingle(final PeerConnection peerConnection, final FutureResponse futureResponse) {
		LOG.debug("Acquire exclusively peerConnoction {} for message {}", peerConnection, futureResponse.request());
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), config);
        final FutureChannelCreator fcc = peerConnection.acquire(futureResponse);
		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
            public void operationComplete(FutureChannelCreator future) throws Exception {
				if(future.isSuccess()) {
	            	requestHandler.sendTCP(peerConnection.channelCreator(), peerConnection);
	            } else {
	            	futureResponse.failed(future);
	            }
            }
		});
        
        return futureResponse;
    }

    /**
     * Set up a relay connection to a peer. If the peer that is asked to act as
     * relay is relayed itself, the request will be denied.
     * 
     * @param channelCreator
     * @param fpcshall
     *            FuturePeerConnection to the peer that shall act as a relay.
     * @return FutureDone with a peer connection to the newly set up relay peer
     */
    public FutureDone<PeerConnection> setupRelay(final ChannelCreator channelCreator, FuturePeerConnection fpc) {
        final FutureDone<PeerConnection> futureDone = new FutureDone<PeerConnection>();
        final Message message = createMessage(fpc.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_1);
        message.keepAlive(true);
        final FutureResponse futureResponse = new FutureResponse(message);
        LOG.debug("Setting up relay connection to peer {}, message {}", fpc.remotePeer(), message);

        fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
            public void operationComplete(final FuturePeerConnection futurePeerConnection) throws Exception {
                if (futurePeerConnection.isSuccess()) {
                	final PeerConnection peerConnection = futurePeerConnection.object();
                	sendSingle(peerConnection, futureResponse).addListener(new BaseFutureAdapter<FutureResponse>() {
                        public void operationComplete(FutureResponse future) throws Exception {
                            if (future.isSuccess()) {
                                futureDone.done(peerConnection);
                            } else {
                                futureDone.failed(future);
                            }
                        }
                    });
                } else {
                    futureDone.failed(futurePeerConnection);
                }
            }
        });
        return futureDone;
    }
    
    //TODO jwa --> forwardConnectionSetup
    @Override
    public void rConnectionSetup(final Message requestMessage, PeerConnection peerConnection, Responder responder) {
    	
    }

    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
        LOG.debug("received RPC message {}", message);
        if (message.type() == Type.REQUEST_1 && message.command() == RPC.Commands.RELAY.getNr()) {
            handleSetup(message, peerConnection, responder);
        } else if (message.type() == Type.REQUEST_2 && message.command() == RPC.Commands.RELAY.getNr()) {
            handlePiggyBackMessage(message, responder);
        } else if (message.type() == Type.REQUEST_3 && message.command() == RPC.Commands.RELAY.getNr()) {
            handleMap(message, responder);
        } else {
            throw new IllegalArgumentException("Message content is wrong");
        }
    }

    public Peer peer() {
        return this.peer;
    }

    private void handleSetup(Message message, final PeerConnection peerConnection, Responder responder) {
        
        if (peerBean().serverPeerAddress().isRelayed()) {
            // peer is behind a NAT as well -> deny request
            responder.response(createResponseMessage(message, Type.DENIED));
            return;
        }

        // register relay forwarder
        RelayForwarderRPC.register(peerConnection, peer, this);

        // add close listener for the peer connection
        peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
            @Override
            public void operationComplete(FutureDone<Void> future) throws Exception {
                // unregister relay handler
            	LOG.debug("Unregister the relay for {}", peerConnection.remotePeer().peerId());
                RelayForwarderRPC.unregister(peer, peerConnection.remotePeer().peerId());
            }
        });

        responder.response(createResponseMessage(message, Type.OK));
    }

    private void handlePiggyBackMessage(Message message, Responder responderToRelay) throws Exception {
        // TODO: check if we have right setup
        Buffer requestBuffer = message.buffer(0);
        Message realMessage = RelayUtils.decodeMessage(requestBuffer, new InetSocketAddress(0), new InetSocketAddress(0));
        LOG.debug("Received message from relay peer: {}", realMessage);
        realMessage.restoreContentReferences();
        NoDirectResponse responder = new NoDirectResponse();
        // TODO: Not sure what to do with the peer connection and sign
        peer.connectionBean().dispatcher().associatedHandler(realMessage).handleResponse(realMessage, null, false, responder);
        LOG.debug("Send reply message to relay peer: {}", responder.response());
        Message response = createResponseMessage(message, Type.OK);
        response.buffer(RelayUtils.encodeMessage(responder.response()));
        responderToRelay.response(response);
    }

    /**
     * Updates the peer map of an unreachable peer on the relay peer, so that
     * the relay peer can respond to neighbor RPC on behalf of the unreachable
     * peer
     * 
     * @param message
     * @param responder
     */
    private void handleMap(Message message, Responder responder) {
        Collection<PeerAddress> map = message.neighborsSet(0).neighbors();
        RelayForwarderRPC relayForwarderRPC = RelayForwarderRPC.find(peer, message.sender().peerId());
        if (relayForwarderRPC != null) {
            relayForwarderRPC.setMap(RelayUtils.unflatten(map, message.sender()));
        } else {
            LOG.error("need to call setup relay first");
        }
        Message response = createResponseMessage(message, Type.OK);
        responder.response(response);
    }
}
