package net.tomp2p.relay;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.android.GCMForwarderRPC;
import net.tomp2p.relay.tcp.OpenTCPForwarderRPC;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);
    private final ConnectionConfiguration config;
    private final Peer peer;
	
    // used when it should serve as an Android relay server
    private final String gcmAuthToken;
	
	// holds the forwarder for each client
	private final Map<Number160, BaseRelayForwarderRPC> forwarders;

    /**
     * Register the RelayRPC. After the setup, the peer is ready to act as a
     * relay if asked by an unreachable peer.
     * 
     * @param peer
     *            The peer to register the RelayRPC
     * @return
     */
    public RelayRPC(Peer peer, String gcmAuthToken) {
        super(peer.peerBean(), peer.connectionBean());
		this.gcmAuthToken = gcmAuthToken;
        this.peer = peer;
        this.config = new DefaultConnectionConfiguration();
        this.forwarders = new ConcurrentHashMap<Number160, BaseRelayForwarderRPC>();
        
        // register this handler
        register(RPC.Commands.RELAY.getNr());
    }
    
	public ConnectionConfiguration config() {
		return config;
	}
    
    public FutureResponse sendToRelayPeer(PeerConnection connection, Message message) {
    	return RelayUtils.send(connection, peerBean(), connectionBean(), config, message);
    }

    /**
     * Set up a relay connection to a peer. If the peer that is asked to act as
     * relay is relayed itself, the request will be denied.
     * 
     * @param channelCreator
     * @param gcmRegistrationId 
     * @param fpcshall
     *            FuturePeerConnection to the peer that shall act as a relay.
     * @return FutureDone with a peer connection to the newly set up relay peer
     */
    public FutureDone<PeerConnection> setupRelay(final ChannelCreator channelCreator, final FuturePeerConnection fpc, RelayType relayType, String gcmRegistrationId) {
        final FutureDone<PeerConnection> futureDone = new FutureDone<PeerConnection>();
        
        final Message message;
        if(relayType == RelayType.OPENTCP) {
        	message = createMessage(fpc.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_1);
        } else if(relayType == RelayType.ANDROID) {
        	message = createMessage(fpc.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_4);
        	message.buffer(RelayUtils.encodeRegistrationId(gcmRegistrationId));
        } else {
        	throw new IllegalArgumentException("Unknown relay type " + relayType);
        }
        
        // depend on the relay type whether to keep the connection open or close it after the setup.
        message.keepAlive(relayType.keepConnectionOpen());

        
        LOG.debug("Setting up relay connection to peer {}, message {}", fpc.remotePeer(), message);

        fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
            public void operationComplete(final FuturePeerConnection futurePeerConnection) throws Exception {
                if (futurePeerConnection.isSuccess()) {
                	// successfully created a connection to the relay peer
                	final PeerConnection peerConnection = futurePeerConnection.object();
                	FutureResponse response = sendToRelayPeer(peerConnection, message);
                	response.addListener(new BaseFutureAdapter<FutureResponse>() {
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

    /**
     * Receive a message at the relay server and the relay client
     */
    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
        LOG.debug("received RPC message {}", message);
        if (message.type() == Type.REQUEST_1 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// request from unreachable peer to the relay
            handleSetup(message, peerConnection, responder);
        } else if (message.type() == Type.REQUEST_4 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// request from mobile device to the relay
        	handleSetupAndroid(message, peerConnection, responder);
        } else if (message.type() == Type.REQUEST_2 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// The unreachable peer receives wrapped messages from the relay
            handlePiggyBackMessage(message, responder);
        } else if (message.type() == Type.REQUEST_3 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// the relay server receives the update of the routing table regularly from the unrachable peer
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
        	LOG.warn("I cannot be a relay since I'm relayed as well! {}", message);
            responder.response(createResponseMessage(message, Type.DENIED));
            return;
        }

        // register relay forwarder
        OpenTCPForwarderRPC tcpForwarder = new OpenTCPForwarderRPC(peerConnection, peer, config);
        registerRelayForwarder(tcpForwarder);

        LOG.debug("I'll be your relay! {}", message);
        responder.response(createResponseMessage(message, Type.OK));
    }
    
    /** 
     * An android device is behind a firewall and wants to be relayed 
     */
    private void handleSetupAndroid(Message message, final PeerConnection peerConnection, Responder responder) {
        Buffer buffer = message.buffer(0);
        if(buffer == null || buffer.buffer() == null) {
        	LOG.error("Device {} did not send any GCM registration id", peerConnection.remotePeer());
            responder.response(createResponseMessage(message, Type.DENIED));
            return;
        }
        
		String registrationId = RelayUtils.decodeRegistrationId(buffer);
		if(registrationId == null) {
			LOG.error("Cannot decode the registrationID from the message");
            responder.response(createResponseMessage(message, Type.DENIED));
            return;
		}
		
		if(gcmAuthToken == null) {
			LOG.error("This relay peer is not capable to serve Android devices");
			responder.response(createResponseMessage(message, Type.DENIED));
	        return;
		}
        
		LOG.debug("Hello Android device! You'll be relayed over GCM. {}", message);
		GCMForwarderRPC forwarderRPC = new GCMForwarderRPC(peer, peerConnection, gcmAuthToken, registrationId);
		registerRelayForwarder(forwarderRPC);
    }
    
    private void registerRelayForwarder(BaseRelayForwarderRPC forwarder) {
		for (Commands command : RPC.Commands.values()) {
			if (command != RPC.Commands.RELAY) {
				peer.connectionBean().dispatcher()
				        .registerIoHandler(forwarder.unreachablePeerAddress().peerId(), forwarder, command.getNr());
			}
		}
		
		peer.peerBean().addPeerStatusListeners(forwarder);
		forwarders.put(forwarder.unreachablePeerId(), forwarder);
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
    	LOG.debug("handle foreign map {}", message);
        Collection<PeerAddress> map = message.neighborsSet(0).neighbors();
        BaseRelayForwarderRPC forwarder = forwarders.get(message.sender().peerId());
        if (forwarder != null) {
        	forwarder.setMap(RelayUtils.unflatten(map, message.sender()));
        } else {
            LOG.error("No forwarder for peer {} found. Need to setup relay first");
        }
        
        Message response = createResponseMessage(message, Type.OK);
        responder.response(response);
    }
}
