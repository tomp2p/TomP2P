package net.tomp2p.connection;

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Triple;

public interface ChannelSender {
	public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> send(Message message);
}
