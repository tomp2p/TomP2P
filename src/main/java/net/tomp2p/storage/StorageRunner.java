package net.tomp2p.storage;
import net.tomp2p.peers.Number160;

public interface StorageRunner
{
	public abstract void call(Number160 locationKey, Number160 domainKey, Number160 contentKey,
			Data data);
}
