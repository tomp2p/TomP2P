package net.tomp2p.connection;

import net.tomp2p.message.Message;

public interface Responder {

	void response(Message responseMessage);
	
	void failed(String reason);
}