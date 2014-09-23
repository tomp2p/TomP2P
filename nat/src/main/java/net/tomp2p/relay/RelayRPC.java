package net.tomp2p.relay;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rcon.RconRPC;
import net.tomp2p.relay.android.AndroidForwarderRPC;
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
	 * This variable is needed, because a relay overwrites every RPC of an
	 * unreachable peer with another RPC called {@link RelayForwarderRPC}. This
	 * variable is forwarded to the {@link RelayForwarderRPC} in order to
	 * guarantee the existence of a {@link RconRPC}. Without this variable, no
	 * reverse connections would be possible.
	 * 
	 * @author jonaswagner
	 */
	private final RconRPC rconRPC;

	/**
     * Register the RelayRPC. After the setup, the peer is ready to act as a
     * relay if asked by an unreachable peer.
     * 
     * @param peer
     *            The peer to register the RelayRPC
     * @param rconRPC the reverse connection RPC
     * @param gcmAuthToken the authentication key for Google cloud messaging
     * @return
     */
	public RelayRPC(Peer peer, RconRPC rconRPC, String gcmAuthToken) {
        super(peer.peerBean(), peer.connectionBean());
		this.gcmAuthToken = gcmAuthToken;
        this.peer = peer;
        this.config = new DefaultConnectionConfiguration();
        this.forwarders = new ConcurrentHashMap<Number160, BaseRelayForwarderRPC>();
        this.rconRPC = rconRPC;
        
        // register this handler
        register(RPC.Commands.RELAY.getNr());
    }
    
	public ConnectionConfiguration config() {
		// TODO move to PeerNAT
		return config;
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
            handlePiggyBackedMessage(message, responder);
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
		AndroidForwarderRPC forwarderRPC = new AndroidForwarderRPC(peer, peerConnection, gcmAuthToken, registrationId);
		registerRelayForwarder(forwarderRPC);
    }
    
    private void registerRelayForwarder(BaseRelayForwarderRPC forwarder) {
		for (Commands command : RPC.Commands.values()) {
			if(command == RPC.Commands.RCON) {
				// We must register the rconRPC for every unreachable peer that
				// we serve as a relay. Without this registration, no reverse
				// connection setup is possible.
				peer.connectionBean().dispatcher()
						.registerIoHandler(forwarder.unreachablePeerId(), rconRPC, command.getNr());
			} else if (command == RPC.Commands.RELAY) {
				// don't register the relay command
				continue;
			} else {
				peer.connectionBean().dispatcher().registerIoHandler(forwarder.unreachablePeerId(), forwarder, command.getNr());
			}
		}
		
		peer.peerBean().addPeerStatusListeners(forwarder);
		forwarders.put(forwarder.unreachablePeerId(), forwarder);
	}

    /**
     * The unreachable peer received an envelope message with another message insice (piggypacked)
     */
    private void handlePiggyBackedMessage(final Message message, final Responder responderToRelay) throws Exception {
        // TODO: check if we have right setup
        Buffer requestBuffer = message.buffer(0);
        Message realMessage = RelayUtils.decodeMessage(requestBuffer, message.recipientSocket(), message.senderSocket());
        LOG.debug("Received message from relay peer: {}", realMessage);
        realMessage.restoreContentReferences();
        
        final Responder responder = new Responder() {
        	
        	//TODO: add reply leak handler
        	@Override
        	public void response(Message responseMessage) {
        		Message envelope = createResponseMessage(message, Type.OK);
        		LOG.debug("Send reply message to relay peer: {}", responseMessage);
        		try {
	                envelope.buffer(RelayUtils.encodeMessage(responseMessage));
                } catch (Exception e) {
                	LOG.error("Cannot piggyback the response", e);
                	failed(Type.EXCEPTION, e.getMessage());
                }
                responderToRelay.response(envelope);
        	}

			@Override
            public void failed(Type type, String reason) {
				responderToRelay.failed(type, reason);
            }

			@Override
            public void responseFireAndForget() {
				responderToRelay.responseFireAndForget();
            }
        };
        
        // TODO: Not sure what to do with the peer connection and sign
        DispatchHandler dispatchHandler = peer.connectionBean().dispatcher().associatedHandler(realMessage);
        if(dispatchHandler == null) {
        	responder.failed(Type.EXCEPTION, "handler not found, probably not relaying peer anymore");
        } else {
        	dispatchHandler.handleResponse(realMessage, null, false, responder);
        }
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
    	LOG.debug("Handle foreign map update {}", message);
        Collection<PeerAddress> map = message.neighborsSet(0).neighbors();
        BaseRelayForwarderRPC forwarder = forwarders.get(message.sender().peerId());
        if (forwarder != null) {
        	forwarder.setPeerMap(RelayUtils.unflatten(map, message.sender()));
        } else {
            LOG.error("No forwarder for peer {} found. Need to setup relay first");
        }
        
        Message response = createResponseMessage(message, Type.OK);
        responder.response(response);
    }
}
