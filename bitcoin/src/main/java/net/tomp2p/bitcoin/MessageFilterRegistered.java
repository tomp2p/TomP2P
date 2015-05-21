package net.tomp2p.bitcoin;

import net.tomp2p.message.Message;
import net.tomp2p.message.MessageFilter;
import net.tomp2p.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filter that can prevent messages from being dispatched that originate
 * from peers without a verified registration.
 * Therefore request from peers that are not registered will be ignored.
 *
 * @author Alexander Mülli
 *
 */
public class MessageFilterRegistered implements MessageFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MessageFilterRegistered.class);

    private RegistrationService regServ;
    private RegistrationStorage regStore;

    public MessageFilterRegistered(RegistrationService regServ, RegistrationStorage regStore) {
        this.regServ = regServ;
        this.regStore = regStore;
    }

    @Override
    public Pair<Boolean, Message> rejectPreDispatch(Message message) {
        Registration registration = new Registration(message);
        //check local registration storage if registration was already verified
        if(regStore.lookup(registration))
            return new Pair<Boolean, Message>(false, null);
        //else verify registraion on blockchain
        else if(regServ.verify(registration))
            return new Pair<Boolean, Message>(false, null);
        else
            LOG.debug("Message denied");
            return new Pair<Boolean, Message>(true, createResponseMessage(message));
    }

    private Message createResponseMessage(Message requestMessage) {
        Message replyMessage = new Message();
        replyMessage.senderSocket(requestMessage.senderSocket());
        replyMessage.recipientSocket(requestMessage.recipientSocket());
        replyMessage.recipient(requestMessage.sender());
        replyMessage.command(requestMessage.command());
        replyMessage.type(Message.Type.DENIED);
        replyMessage.version(requestMessage.version());
        replyMessage.messageId(requestMessage.messageId());
        replyMessage.udp(requestMessage.isUdp());
        return replyMessage;
    }
}
