package net.tomp2p.bitcoin;

import net.tomp2p.peers.Number160;
import org.bitcoinj.core.Sha256Hash;

import java.security.PublicKey;

public class Registration {
    private Number160 peerId;
    private Sha256Hash transactionId;
    private Sha256Hash blockId;
    private PublicKey publicKey;

    public Registration() {
    }

    // constructor for testing
    public Registration(Number160 peerId, Sha256Hash blockId, Sha256Hash transactionId, PublicKey publicKey) {
        this.peerId = peerId;
        this.transactionId = transactionId;
        this.blockId = blockId;
        this.publicKey = publicKey;
    }

    public Number160 getPeerId() {
        return peerId;
    }

    public void setPeerId(Number160 peerId) {
        this.peerId = peerId;
    }

    public Sha256Hash getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Sha256Hash transactionId) {
        this.transactionId = transactionId;
    }

    public Sha256Hash getBlockId() {
        return blockId;
    }

    public void setBlockId(Sha256Hash blockId) {
        this.blockId = blockId;
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(PublicKey publicKey) {
        this.publicKey = publicKey;
    }

}
