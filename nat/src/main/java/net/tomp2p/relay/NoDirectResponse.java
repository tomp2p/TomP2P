package net.tomp2p.relay;

import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;

/**
 * Responder that doesn't respond to the Sender of a Message, but saves the
 * response message. This class is used for unreachable peers that are connected
 * to a relay, so they don't reply to the original Sender of a message but to a
 * relay peer
 * 
 * @author Raphael Voellmy
 * 
 */
class NoDirectResponse implements Responder {

    private Message response;

    /**
     * Saves the response message. The response message can be retrieved usig {@link NoDirectResponse#getResponse()}
     */
    public void response(Message responseMessage) {
        this.response = responseMessage;
    }

    /**
     * Retrieves the response message
     * 
     * @return the response message
     */
    public Message getResponse() {
        return response;
    }

    /**
     * <strong>Do not use!</strong> This method doesn't do anything.
     */
    public void failed(Type type, String reason) {
        // do nothing
    }

    /**
     * <strong>Do not use!</strong> This method doesn't do anything.
     */
    public void responseFireAndForget() {
        // do nothing
    }
}