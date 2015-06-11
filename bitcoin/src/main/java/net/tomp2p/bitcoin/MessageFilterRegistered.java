package net.tomp2p.bitcoin;

import net.tomp2p.message.Message;
import net.tomp2p.message.MessageFilter;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filter that can prevent messages from being dispatched that originate
 * from peers without a verified registration. The Message is also checked
 * Therefore request from peers that are not registered will be ignored.
 * The Message is also checked for authenticity, meaning if the message is
 * signed with the same key the registration is based on.
 *
 * @author Alexander Mülli
 *
 */
public class MessageFilterRegistered implements MessageFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MessageFilterRegistered.class);

    private RegistrationService regServ;
    private RegistrationStorage regStore;

    public MessageFilterRegistered(RegistrationService regServ) {
        this.regServ = regServ;
    }

    @Override
    public Pair<Boolean, Message> rejectPreDispatch(Message message) {
        if(message.hasHeaderExtension()) {
            RegistrationBitcoin registration = new RegistrationBitcoin(message);
            //verify registration and that message is authentic
            if (regServ.verify(registration)) {
                LOG.debug("registration of message sender verified");
                if(regServ.authenticate(registration, message)) {
                    LOG.debug("registration of message sender authenticated");
                    return new Pair<Boolean, Message>(false, null);
                }
            }
        }
        LOG.debug("Message denied");
        return new Pair<Boolean, Message>(true, createResponseMessage(message));
    }

    private Message createResponseMessage(Message requestMessage) {
        return DispatchHandler.createResponseMessage(requestMessage, Message.Type.DENIED, requestMessage.sender());
    }
}
