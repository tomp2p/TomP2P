package net.tomp2p.bitcoin;

import com.google.common.util.concurrent.AbstractIdleService;
import net.tomp2p.peers.Number160;
import org.bitcoinj.core.Sha256Hash;

public class Registration extends AbstractIdleService {
    private Number160 peerId;
    private Sha256Hash transactionId;
    private Sha256Hash blockId;

    public Registration() {
    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void shutDown() throws Exception {

    }

    public Registration(Number160 peerId, Sha256Hash blockId, Sha256Hash transactionId) {
        this.peerId = peerId;
        this.transactionId = transactionId;
        this.blockId = blockId;
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
}
