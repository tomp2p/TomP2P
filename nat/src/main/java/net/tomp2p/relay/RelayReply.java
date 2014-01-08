package net.tomp2p.relay;

import java.net.InetSocketAddress;

import net.tomp2p.connection.Dispatcher;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RawDataReply;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayReply implements RawDataReply {
	
	private final static Logger logger = LoggerFactory.getLogger(RelayReply.class);
	private final Dispatcher dispatcher;
	
	public RelayReply(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception {
		Message message = RelayUtils.decodeMessage(requestBuffer, new InetSocketAddress(0), new InetSocketAddress(0));
		if(message.getCommand() == 7) {
			System.err.println("relay reply");
		}
		logger.debug("Received message from relay peer: {}", message);
		message.restoreContentReferences();

        NoDirectResponse responder = new NoDirectResponse();
        
        //TODO: Not sure what to do with the peer connection and sign
        dispatcher.getAssociatedHandler(message).handleResponse(message, null, false, responder);
        logger.debug("Send reply message to relay peer: {}", responder.getResponse());
		return RelayUtils.encodeMessage(responder.getResponse());
	}

}
