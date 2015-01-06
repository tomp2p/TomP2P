package net.tomp2p.examples.relay;

import java.security.PublicKey;

import net.tomp2p.dht.StorageLayer;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prints logs when data is stored. Can be set to deny any storage.
 * 
 * @author Nico Rutishauser
 *
 */
public class LoggingStorageLayer extends StorageLayer {

	private static final Logger LOG = LoggerFactory.getLogger(LoggingStorageLayer.class);
	private final String peerName;
	private final boolean accept;

	public LoggingStorageLayer(String peerName, boolean accept) {
		super(new StorageMemory());
		this.peerName = peerName;
		this.accept = accept;
	}

	@Override
	public Enum<?> put(Number640 key, Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection, boolean selfSend) {
		if (accept) {
			LOG.debug("{}: Putting data {} to key {}", peerName, newData, key);
			return super.put(key, newData, publicKey, putIfAbsent, domainProtection, selfSend);
		} else {
			LOG.debug("{}: Denying to put data {} to key {}", peerName, newData, key);
			return PutStatus.FAILED;
		}
	}

}
