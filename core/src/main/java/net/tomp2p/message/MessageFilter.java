package net.tomp2p.message;

import net.tomp2p.utils.Pair;

/**
 * A filter that can prevent messages from being dispatched.
 * Therefore the contained request will be ignored.
 *
 * @author Alexander Mülli
 *
 */
public interface MessageFilter {

    /**
     * Each message that is about to be dispatched is run through this filter in advance.
     * @param message
     *              The message that is about to be dispatched
     * @return Pair of Boolean and Message. Boolean is true if the message is rejected and should not be dispatched, false otherwise.
     * The Message will be sent as response if the incoming message is rejected.
     *
     */
    Pair<Boolean, Message> rejectPreDispatch(Message message);
}
