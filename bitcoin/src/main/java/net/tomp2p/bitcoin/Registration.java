package net.tomp2p.bitcoin;

import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import org.bitcoinj.core.Sha256Hash;

import java.security.KeyPair;
import java.security.PublicKey;

public class Registration implements net.tomp2p.p2p.Registration {
    private Number160 peerId;
    private Sha256Hash transactionId;
    private Sha256Hash blockId;
    private KeyPair keyPair;
    private PublicKey publicKey;

    public Registration() {
    }

    /**
     * Constructor for Registration with data from {@link Message}
     * @param message
     */
    public Registration(Message message) {
        this.peerId = message.sender().peerId();
        decodeHeaderExtension(message.headerExtension());
        this.publicKey = message.publicKey(0);
    }

    // constructor for testing
    public Registration(Number160 peerId, Sha256Hash blockId, Sha256Hash transactionId, PublicKey publicKey) {
        this.peerId = peerId;
        this.transactionId = transactionId;
        this.blockId = blockId;
        this.publicKey = publicKey;
    }

    @Override
    public byte[] encodeHeaderExtension() {
        byte[] encodeHeaderExtension = new byte[64];
        byte[] blockIdBytes = this.blockId.getBytes();
        byte[] transactionIdBytes = this.transactionId.getBytes();
        System.arraycopy(blockIdBytes, 0, encodeHeaderExtension, 0, blockIdBytes.length);
        System.arraycopy(transactionIdBytes, 0, encodeHeaderExtension, blockIdBytes.length, transactionIdBytes.length);
        return encodeHeaderExtension;
    }

    private void decodeHeaderExtension(byte[] headerExtension) {
        byte[] blockIdBytes = new byte[32];
        byte[] transactionIdBytes = new byte[32];
        System.arraycopy(headerExtension, 0, blockIdBytes, 0, blockIdBytes.length);
        System.arraycopy(headerExtension, blockIdBytes.length, transactionIdBytes, 0, transactionIdBytes.length);
        this.blockId = new Sha256Hash(blockIdBytes);
        this.transactionId = new Sha256Hash(transactionIdBytes);
    }

    @Override
    public KeyPair getKeyPair() {
        return keyPair;
    }

    public void setKeyPair(KeyPair keyPair) {
        this.keyPair = keyPair;
    }

    @Override
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
        if(keyPair != null)
            return keyPair.getPublic();
        else
            return publicKey;
    }

    public void setPublicKey(PublicKey publicKey) {
        this.publicKey = publicKey;
    }

}
