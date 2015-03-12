package net.tomp2p.dht;

import java.util.Collection;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public interface SearchableBuilder {

	Number640 from();

	Number640 to();

	Collection<Number160> contentKeys();

}
