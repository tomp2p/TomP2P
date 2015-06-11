package net.tomp2p.bitcoin;

import net.tomp2p.message.Message;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import org.bitcoinj.core.Sha256Hash;

import java.io.Serializable;
import java.security.KeyPair;
import java.security.PublicKey;

public class RegistrationBitcoin implements Registration, Serializable {
    private Number160 peerId;
    private Sha256Hash transactionId;
    private Sha256Hash blockId;
    private KeyPair keyPair; //used when new Peer is generated based on new registration
    private PublicKey publicKey; //used when registration is decoded form message/peerAddress and verified

    private final int SIZE = 64;

    public RegistrationBitcoin() {
    }

    /**
     * Constructor for Registration with data from {@link Message}
     * @param message
     */
    public RegistrationBitcoin(Message message) {
        this.peerId = message.sender().peerId();
        decode(message.headerExtension());
    }

    public RegistrationBitcoin(PeerAddress peerAddress) {
        this.peerId = peerAddress.peerId();
        decode(peerAddress.getRegistration());
    }

    // constructor for testing
    public RegistrationBitcoin(Number160 peerId, Sha256Hash blockId, Sha256Hash transactionId, PublicKey publicKey) {
        this.peerId = peerId;
        this.transactionId = transactionId;
        this.blockId = blockId;
        this.publicKey = publicKey;
    }

    @Override
    public byte[] encode() {
        byte[] encodeHeaderExtension = new byte[64];
        byte[] blockIdBytes = this.blockId.getBytes();
        byte[] transactionIdBytes = this.transactionId.getBytes();
        System.arraycopy(blockIdBytes, 0, encodeHeaderExtension, 0, blockIdBytes.length);
        System.arraycopy(transactionIdBytes, 0, encodeHeaderExtension, blockIdBytes.length, transactionIdBytes.length);
        return encodeHeaderExtension;
    }

    @Override
    public int size() {
        return SIZE;
    }

    private void decode(byte[] headerExtension) {
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


    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof RegistrationBitcoin)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        final RegistrationBitcoin reg = (RegistrationBitcoin) obj;
        if(!reg.getPeerId().equals(this.getPeerId())) {
            return false;
        }
        if(!reg.getBlockId().equals(this.getBlockId())) {
           return false;
        }
        if(!reg.getTransactionId().equals(this.getTransactionId())) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("reg");
        sb.append("/peerId(").append(peerId).append(")");
        sb.append("/blockId(").append(blockId).append(")");
        sb.append("/transactionId(").append(transactionId).append(")");
        return sb.toString();
    }

//    @Override
    //TODO implement hashCode
//    public int hashCode() {
//        int hashCode = 0;
//        for (int i = 0; i < INT_ARRAY_SIZE; i++) {
//            hashCode = (int) (31 * hashCode + (val[i] & LONG_MASK));
//        }
//        return hashCode;
//    }
}
