package net.tomp2p.connection;

import net.tomp2p.message.Message;

public interface Responder {

	public abstract void response(Message responseMessage);
	
	public abstract void failed(String reason);
}