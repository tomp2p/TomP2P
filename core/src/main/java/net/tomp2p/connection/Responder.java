package net.tomp2p.connection;

import net.tomp2p.message.Message;

public interface Responder {

	void response(Message responseMessage);

	void failed(Message.Type type, String reason);

	void responseFireAndForget();

}