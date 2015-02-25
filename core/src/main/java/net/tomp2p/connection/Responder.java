package net.tomp2p.connection;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;

public interface Responder {

	public abstract FutureDone<Void> response(Message responseMessage);
	
	public abstract void failed(Message.Type type, String reason);

	public abstract void responseFireAndForget();

}