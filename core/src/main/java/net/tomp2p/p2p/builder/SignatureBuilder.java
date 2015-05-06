package net.tomp2p.p2p.builder;

import java.security.KeyPair;

public interface SignatureBuilder<K extends SignatureBuilder<K>> {

    /**
     * Indicates whether a message should be signed or not.
     * @return
     */
    public abstract boolean isSign();

    /**
     * Sets whether a message should be signed or not.
     * For protecting an entry, this needs to be set to true.
     * @param signMessage
     *            True, if the message should be signed.
     * @return This instance
     */
    public abstract K sign(boolean signMessage);

    /**
     * Sets the message to be signed.
     * @return This instance
     */
    public abstract K sign();

    /**
     * Sets the key pair to sing the message. The key will be attached to the message and stored
     * potentially with a data object (if there is such an object in the message).
     * @param keyPair
     *            The key pair to be used for signing.
     * @return This instance
     */
    public abstract K keyPair(KeyPair keyPair);

    /**
     * Gets the current key pair used to sign the message. If null, no signature is applied.
     * @return
     */
    public abstract KeyPair keyPair();

}