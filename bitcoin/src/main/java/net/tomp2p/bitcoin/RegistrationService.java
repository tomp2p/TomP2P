package net.tomp2p.bitcoin;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;
import org.bitcoinj.core.*;
import org.bitcoinj.kits.WalletAppKit;
import org.bitcoinj.params.RegTestParams;
import org.bitcoinj.params.TestNet3Params;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.bitcoinj.script.ScriptOpCodes.OP_RETURN;

public class RegistrationService {

    private final static Logger LOG = LoggerFactory.getLogger(RegistrationService.class);
    private RegistrationStorage storage;
    public WalletAppKit kit;
    protected FutureDone<RegistrationBitcoin> registration;

    /**
     * Registration Service
     * @param storage implementation of registration storage for previously verified registration (caching)
     * @param params bitcoin network params (MainNet, TestNet or RegTest)
     * @param dir directory for storage of spv wallet
     * @param filename filename for spv wallet
     */
    public RegistrationService(RegistrationStorage storage, NetworkParameters params, File dir, String filename) {
        this.storage = storage;
        this.kit = new WalletAppKit(params, dir, filename);
    }

    public RegistrationService start() throws ExecutionException, InterruptedException {
        storage.start();
        kit.startAsync();
        LOG.debug("starting Registration Service");
        kit.awaitRunning();
        kit.peerGroup().waitForPeers(1).get();
        List<Peer> peers = kit.peerGroup().getConnectedPeers();
        LOG.debug("connected to peers: {}", peers);
        LOG.debug("Registration Service running");
        return this;
    }

    public void stop() {
        storage.stop();
        kit.stopAsync();
        kit.awaitTerminated();
        LOG.debug("WalletAppKit stopped");
    }

    /**
     * Registers public key with a transaction in the blockchain.
     * @param keyPair keyPair with public key for peer registration
     */
    public FutureDone<RegistrationBitcoin> registerPeer(final KeyPair keyPair) throws InterruptedException, ExecutionException {
        registration = new FutureDone<RegistrationBitcoin>();
        Number160 peerId = null;
        final RegistrationBitcoin reg = new RegistrationBitcoin();
        reg.setKeyPair(keyPair);
        Coin value = Coin.parseCoin("0.001");

        LOG.info("Wallet Balance: " + kit.wallet().getBalance() + " Satoshis");
        //check for sufficient funds in wallet
        if(kit.wallet().getBalance().isLessThan(value)) {
            LOG.info("Not enough coins in your wallet. Missing " + value.subtract(kit.wallet().getBalance()) + " satoshis are missing (including fees)");
            LOG.info("Send money to: " + kit.wallet().currentReceiveAddress().toString());
            //TODO: calculate missing coins correctly with fees
            ListenableFuture<Coin> balanceFuture = kit.wallet().getBalanceFuture(value, Wallet.BalanceType.AVAILABLE);
            FutureCallback<Coin> callback = new FutureCallback<Coin>() {
                public void onSuccess(Coin balance) {
                    LOG.info("coins arrived and the wallet now has enough balance");
                }

                public void onFailure(Throwable t) {
                    LOG.error("something went wrong");
                }
            };
            Futures.addCallback(balanceFuture, callback);
            balanceFuture.get(); // blocks until funds are sufficient
        }

        //create transaction with public key in output script
        Transaction tx = new Transaction(TestNet3Params.get());
        Number160 pubKeyHash = net.tomp2p.utils.Utils.makeSHAHash(keyPair.getPublic().getEncoded());
        LOG.debug("write public key " + pubKeyHash + " into transaction");
        Script script = new ScriptBuilder().op(OP_RETURN).data(keyPair.getPublic().getEncoded()).build();
//        tx.addOutput(Transaction.MIN_NONDUST_OUTPUT, script);
        tx.addOutput(Coin.ZERO, script);

        //broadcast transaction
        Wallet.SendRequest sendRequest = Wallet.SendRequest.forTx(tx);
        try {
            kit.wallet().sendCoins(sendRequest);
        } catch (InsufficientMoneyException e) {
            LOG.info("Not enough coins in your wallet. " + e.missing.getValue() + " satoshis are missing (including fees)");
        }
        reg.setTransactionId(tx.getHash());
        LOG.debug("transaction {} broadcasted", tx.getHash());
        if(kit.params() != RegTestParams.get()) {
            LOG.debug("http://explorer.chain.com/transactions/" + tx.getHash());
        }
        LOG.debug("waiting for transaction to be included in block");
        //add Listener for when transaction is included in blockchain
        tx.getConfidence().addEventListener(new TransactionConfidence.Listener() {
            @Override
            public void onConfidenceChanged(Transaction tx, ChangeReason reason) {
                TransactionConfidence confidence = tx.getConfidence();
                if (reason.equals(ChangeReason.TYPE) && confidence.getConfidenceType().equals(TransactionConfidence.ConfidenceType.BUILDING)) {
                    Peer peer = kit.peerGroup().getConnectedPeers().get(0);
                    Sha256Hash blockHash = null;
                    //TODO: find a cleaner way to get hash of block where transaction is included
                    for (Sha256Hash hash : tx.getAppearsInHashes().keySet()) blockHash = hash;
                    reg.setBlockId(blockHash);
                    LOG.debug("transaction {} has been included in block {}", tx.getHash(), blockHash);
                    if(kit.params() != RegTestParams.get()) {
                        LOG.debug("http://explorer.chain.com/blocks/" + blockHash);
                    }
                    try {
                        //download the full block where transaction was included
                        Block b = peer.getBlock(blockHash).get();
                        //generate peerId
                        Number160 peerId = generatePeerId(keyPair.getPublic(), b.getNonce());
                        reg.setPeerId(peerId);
                        registration.done(reg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        registration.await();
        LOG.debug("PeerId in registration object: {}", registration.object().getPeerId().toString());
        return registration;
    }

    /**
     * Validates if the supplied peerId is based on public key in the transaction and the block nonce
     *
     * @return true if verification was successful
     */
    public boolean verify(RegistrationBitcoin registration) {
        //check local registration storage if registration was already verified
        if(storage.lookup(registration)) {
            LOG.debug("registration was already verified");
            return true;
        }
        //else verify registration on blockchain
        Number160 peerId = registration.getPeerId();
        LOG.debug("registration of peer {} unkown or has changed", peerId);
        LOG.debug("verifying registration of peer " + peerId);
        Sha256Hash blockHash = registration.getBlockId();
        Sha256Hash transactionHash = registration.getTransactionId();
        Block b = null;
        try {
            // asking bitcoin peer for block
            Peer peer = kit.peerGroup().getConnectedPeers().get(0);
            LOG.debug("trying to get block " + blockHash);
            b = peer.getBlock(blockHash).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.debug("block nonce: " + b.getNonce());
        for(Transaction tx : b.getTransactions()) {
            LOG.debug(tx.getHash().toString());
            if (tx.getHash().equals(transactionHash)) {
                LOG.debug("transaction found inside block");
                //TODO: option to validate transaction (e.g. amount and output address)
                for(TransactionOutput output : tx.getOutputs()) {
                    if (output.getScriptPubKey().isOpReturn()) {
                        LOG.debug(output.getScriptPubKey().toString());
                        byte[] pubKeyEncoded = output.getScriptPubKey().getChunks().get(1).data;
                        LOG.debug("found public key in transaction: " + net.tomp2p.utils.Utils.makeSHAHash(pubKeyEncoded));
                        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyEncoded);
                        KeyFactory keyFactory = null;
                        try {
                            keyFactory = KeyFactory.getInstance("DSA");
                            PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);
                            //check if peerId is based on block nonce and public key
                            if (generatePeerId(pubKey, b.getNonce()).equals(peerId)) {
                                //set public key from transaction data and store registration for later reference
                                registration.setPublicKey(pubKey);
                                storage.store(registration);
                                return true;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return false;
                        }
                    }
                }
                break;
            }
        }
        return false;
    }

    /**
     * authenticates if registration corresponds to public key in message
     * @return true if authentic
     */
    public boolean authenticate(Registration registration, Message message) {
        PublicKey publicKey = registration.getPublicKey();
        return publicKey != null && publicKey.equals(message.publicKey(0));
    }

    /**
     * Generates peerId by hashing the public key first, then appending the the block nonce and hash it again.
     *
     * @return generated peerId as Number160
     */
    public Number160 generatePeerId(PublicKey publicKey, Long blockNonce) {
        //TODO: reevaluate way to generate a nodeID from public key and block nonce
        // initialize peerId with SHAHash of public key
        Number160 peerId = Utils.makeSHAHash(publicKey.getEncoded());
        byte[] peerIdBytes = peerId.toByteArray();
        LOG.debug("initial peerId: " + peerId);
        byte[] blockNonceBytes = ByteBuffer.allocate(8).putLong(blockNonce).array();
        // for the seed create a new array that is the size of the two arrays
        byte[] seed = new byte[peerIdBytes.length + blockNonceBytes.length];
        // copy pub key into the seed array (from pos 0, copy pubKeyEnc.length bytes)
        System.arraycopy(peerIdBytes, 0, seed, 0, peerIdBytes.length);
        // copy block nonce into end of seed array (from pos pubKeyEnc.length, copy blockNonceBytes.length bytes)
        System.arraycopy(blockNonceBytes, 0, seed, peerIdBytes.length, blockNonceBytes.length);
        // generate final peerId by hashing the combined byte array
        peerId = Utils.makeSHAHash(seed);
        LOG.debug("block nonce: " + blockNonce);
        LOG.debug("generated peerId: " + peerId);
        return peerId;
    }
}
