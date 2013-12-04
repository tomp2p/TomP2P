package relay;

import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;

class NoDirectResponse implements Responder {
	
	private Message response;

	@Override
	public void response(Message responseMessage) {
		this.response = responseMessage;
	}
	
	public Message getResponse() {
		return response;
	}

	@Override
	public void failed(Type type, String reason) {
		
	}

	@Override
	public void responseFireAndForget() {
		
	}
}