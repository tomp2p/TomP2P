package net.tomp2p.p2p;

import net.tomp2p.peers.Number160;

import java.security.KeyPair;
import java.security.PublicKey;

/**
 * Registration from which a peer can be built.
 *
 * @author Alexander Mülli
 *
 */
public interface Registration {
    /**
     * @return KeyPair that was used for registration
     */
    KeyPair getKeyPair();

    /**
     * @return PublicKey that corresponds to registration
     */
    PublicKey getPublicKey();

    /**
     * @return generated peerId form registration
     */
    Number160 getPeerId();

    /**
     * returns reference that is needed to verify registration
     * @return 64 byte array
     */
    byte[] encode();

    /**
     * returns the size of encoded registration
     * @return size in bytes
     */
    int size();

}
