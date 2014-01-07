package net.tomp2p.relay;

import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;

class NoDirectResponse implements Responder {
	
	private Message response;

	public void response(Message responseMessage) {
		this.response = responseMessage;
	}
	
	public Message getResponse() {
		return response;
	}

	public void failed(Type type, String reason) {
		
	}

	public void responseFireAndForget() {
		
	}
}