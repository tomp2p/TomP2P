package relay;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.net.InetSocketAddress;

import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RawDataReply;

public class RelayReply implements RawDataReply {
	
	private final Dispatcher dispatcher;
	
	public RelayReply(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	@Override
	public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception {
		
		System.err.println("Decoding message");
		Message message = RelayUtils.decodeMessage(requestBuffer);
        
        NoDirectResponse responder = new NoDirectResponse();
        
        //TODO: Not sure what to do with the peer connection and sign
        dispatcher.getAssociatedHandler(message).handleResponse(message, null, false, responder);
        
		return RelayUtils.encodeMessage(responder.getResponse());
	}

}
