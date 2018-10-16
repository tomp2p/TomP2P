package net.tomp2p.connection;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.network.KCP;
import net.tomp2p.utils.Pair;

public interface ChannelSender {
	Pair<FutureDone<Message>, KCP> send(Message message);
}
