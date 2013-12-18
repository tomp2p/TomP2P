package net.tomp2p.p2p.builder;

import java.security.KeyPair;

public interface SignatureBuilder<K extends SignatureBuilder<K>> {

    /**
     * @return Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     */
    public abstract boolean isSign();

    /**
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     * @return This class
     */
    public abstract K sign(boolean signMessage);

    /**
     * @return Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     */
    public abstract K setSign();

    /**
     * @param keyPair
     *            The keyPair to sing the complete message. The key will be attached to the message and stored
     *            potentially with a data object (if there is such an object in the message).
     * @return This class
     */
    public abstract K keyPair(KeyPair keyPair);

    /**
     * @return The current keypair to sign the message. If null, no signature is applied.
     */
    public abstract KeyPair keyPair();

}