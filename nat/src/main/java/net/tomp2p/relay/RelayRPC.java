package net.tomp2p.relay;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.android.AndroidForwarderRPC;
import net.tomp2p.relay.android.AndroidRelayConfiguration;
import net.tomp2p.relay.tcp.OpenTCPForwarderRPC;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);
    private final Peer peer;
    private final ConnectionConfiguration config;
	
    // used when it should serve as an Android relay server
    private final AndroidRelayConfiguration androidConfig;
	
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
	public RelayRPC(Peer peer, RconRPC rconRPC, AndroidRelayConfiguration androidConfig, ConnectionConfiguration config) {
        super(peer.peerBean(), peer.connectionBean());
        this.peer = peer;
		this.androidConfig = androidConfig;
		this.config = config;
        this.forwarders = new ConcurrentHashMap<Number160, BaseRelayForwarderRPC>();
        this.rconRPC = rconRPC;
        
        // register this handler
        register(RPC.Commands.RELAY.getNr());
    }

    /**
     * Receive a message at the relay server and the relay client
     */
    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
        LOG.debug("received RPC message {}", message);
        if (message.type() == Type.REQUEST_1 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// The relay peer receives the setup message from the unreachable peer
        	if(message.intList().isEmpty()) {
                throw new IllegalArgumentException("Setup message should contain an integer value specifying the type");
        	}
        	
        	Integer deviceType = message.intAt(0);
        	if(deviceType == RelayType.OPENTCP.ordinal()) {
        		// request from unreachable peer to the relay
        		handleSetupTCP(message, peerConnection, responder);
        	} else if(deviceType == RelayType.ANDROID.ordinal()) {
        		// request from mobile device to the relay
        		handleSetupAndroid(message, peerConnection, responder);
        	} else {
                throw new IllegalArgumentException("Unknown relay type: " + deviceType);
        	}
        } else if (message.type() == Type.REQUEST_2 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// The unreachable peer receives wrapped messages from the relay
            handlePiggyBackedMessage(message, responder);
        } else if (message.type() == Type.REQUEST_3 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// the relay server receives the update of the routing table regularly from the unrachable peer
            handleMap(message, responder);
        } else if(message.type() == Type.REQUEST_4 && message.command() == RPC.Commands.RELAY.getNr()) {
        	// An android unreachable peer requests the buffer
        	handleBufferRequest(message, responder);
        } else {
            throw new IllegalArgumentException("Message content is wrong");
        }
    }

	public Peer peer() {
        return this.peer;
    }
	
	/**
	 * @return all unreachable peers currently connected to this relay node
	 */
	public Set<PeerAddress> unreachablePeers() {
		Set<PeerAddress> unreachablePeers = new HashSet<PeerAddress>(forwarders.size());
		for (BaseRelayForwarderRPC forwarder : forwarders.values()) {
			unreachablePeers.add(forwarder.unreachablePeerAddress());
		}
		return unreachablePeers;
	}

    /**
     * Open a TCP connection to the unreachable peer
     */
    private void handleSetupTCP(Message message, final PeerConnection peerConnection, Responder responder) {
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
        if(message.bufferList().size() < 2) {
        	LOG.error("Device {} did not send any GCM registration id or the authentication key", peerConnection.remotePeer());
            responder.response(createResponseMessage(message, Type.DENIED));
            return;
        }
        
		String registrationId = RelayUtils.decodeString(message.buffer(0));
		if(registrationId == null) {
			LOG.error("Cannot decode the registrationID from the message");
            responder.response(createResponseMessage(message, Type.DENIED));
            return;
		}
		
		String authenticationKey = RelayUtils.decodeString(message.buffer(1));
		if(authenticationKey == null) {
			LOG.error("Cannot decode the authentication key from the messsage");
			responder.response(createResponseMessage(message, Type.DENIED));
	        return;
		}
		
		int mapUpdateInterval = RelayType.ANDROID.defaultMapUpdateInterval();
		if(message.intAt(1) == null) {
			LOG.warn("Android device did not send the peer map update interval. Take default of {}s", mapUpdateInterval);
		} else {
			mapUpdateInterval = message.intAt(1);
			LOG.debug("Android device sent map update interval of {}s", mapUpdateInterval);
		}
        
		LOG.debug("Hello Android device! You'll be relayed over GCM. {}", message);
		AndroidForwarderRPC forwarderRPC = new AndroidForwarderRPC(peer, peerConnection.remotePeer(), androidConfig, authenticationKey, registrationId, mapUpdateInterval);
		registerRelayForwarder(forwarderRPC);
		
        responder.response(createResponseMessage(message, Type.OK));
    }
    
    private void registerRelayForwarder(BaseRelayForwarderRPC forwarder) {
		for (Commands command : RPC.Commands.values()) {
			if(command == RPC.Commands.RCON) {
				// We must register the rconRPC for every unreachable peer that
				// we serve as a relay. Without this registration, no reverse
				// connection setup is possible.
				peer.connectionBean().dispatcher().registerIoHandler(peer.peerID(), forwarder.unreachablePeerId(), rconRPC, command.getNr());
			} else if (command == RPC.Commands.RELAY) {
				// don't register the relay command
				continue;
			} else {
				peer.connectionBean().dispatcher().registerIoHandler(peer.peerID(), forwarder.unreachablePeerId(), forwarder, command.getNr());
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
        Message realMessage = RelayUtils.decodeMessage(requestBuffer, message.recipientSocket(), message.senderSocket(), connectionBean().channelServer().channelServerConfiguration().signatureFactory());
        LOG.debug("Received message from relay peer: {}", realMessage);
        realMessage.restoreContentReferences();
        
        final Responder responder = new Responder() {
        	
        	//TODO: add reply leak handler
        	@Override
        	public void response(Message responseMessage) {
        		Message envelope = createResponseMessage(message, Type.OK);
        		LOG.debug("Send reply message to relay peer: {}", responseMessage);
        		try {
	                envelope.buffer(RelayUtils.encodeMessage(responseMessage, connectionBean().channelServer().channelServerConfiguration().signatureFactory()));
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
    
    /**
     * The relay buffers messages for unreachable peers (like Android devices). They get notified when the buffer is full
     * or request the buffer content by themselves through this request.
     * 
     * @param message
     * @param responder
     */
    private void handleBufferRequest(Message message, Responder responder) {
    	LOG.debug("Handle buffer request of unreachable peer {}", message.sender());
		BaseRelayForwarderRPC forwarderRPC = forwarders.get(message.sender().peerId());
		if(forwarderRPC instanceof AndroidForwarderRPC) {
			AndroidForwarderRPC androidForwarder = (AndroidForwarderRPC) forwarderRPC;
			
			try {
				Message response = createResponseMessage(message, Type.OK);
				// add all buffered messages
				response.buffer(androidForwarder.getBufferedMessages());
				
				LOG.debug("Responding all buffered messages to Android device {}", message.sender());
				responder.response(response);
			} catch(Exception e) {
				LOG.error("Cannot respond with buffered messages.", e);
				responder.response(createResponseMessage(message, Type.EXCEPTION));
			}
		} else {
			responder.failed(Type.EXCEPTION, "This message type is intended for buffering forwarders only");
		}
	}
}
