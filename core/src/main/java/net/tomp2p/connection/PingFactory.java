package net.tomp2p.connection;

import net.tomp2p.p2p.builder.PingBuilder;

public interface PingFactory {
	PingBuilder ping();
}
