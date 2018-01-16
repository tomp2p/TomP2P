package net.tomp2p.rpc;

import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;

public interface DataCallbackUDP {

	Message data(Message message, Responder r, DirectDataRPC directDataRPC);

}
