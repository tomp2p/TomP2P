package net.tomp2p.bitcoin;

import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;
import org.apache.commons.codec.binary.Hex;
import org.bitcoinj.core.*;
import org.bitcoinj.kits.WalletAppKit;
import org.bitcoinj.params.TestNet3Params;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptBuilder;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.bitcoinj.script.ScriptOpCodes.OP_RETURN;

public class WalletService {

    public WalletAppKit kit;
    protected Registration registration;

    public WalletService(NetworkParameters params, String filename) {
        this.kit = new WalletAppKit(params , new java.io.File("."), filename);
        this.registration = new Registration();
    }

    public void start() throws ExecutionException, InterruptedException {
        kit.startAsync();
        kit.awaitRunning();
        kit.peerGroup().waitForPeers(3).get();
        List<Peer> peers = kit.peerGroup().getConnectedPeers();
        for (Peer p : peers) {
            System.out.println(p.toString());
        }
    }

    public void stop() {
        kit.stopAsync();
        kit.awaitTerminated();
        System.out.println("WalletService stopped");
    }

    public void registerNode(final KeyPair keyPair) throws InsufficientMoneyException, UnsupportedEncodingException, NoSuchAlgorithmException {
        registration.startAsync();
        Number160 peerId = null;
        //create transaction with public key in output script
        Transaction tx = new Transaction(TestNet3Params.get());
        Number160 pubKeyHash = net.tomp2p.utils.Utils.makeSHAHash(keyPair.getPublic().getEncoded());
        System.out.println("write public key " + pubKeyHash + " into transaction");
        Script script = new ScriptBuilder().op(OP_RETURN).data(keyPair.getPublic().getEncoded()).build();
//        tx.addOutput(Transaction.MIN_NONDUST_OUTPUT, script);
        tx.addOutput(Coin.ZERO, script);

        //broadcast transaction
        Wallet.SendRequest sendRequest = Wallet.SendRequest.forTx(tx);
        kit.wallet().sendCoins(sendRequest);
        registration.setTransactionId(tx.getHash());
        System.out.println("transaction broadcasted: http://explorer.chain.com/transactions/" + tx.getHash());

        //add Listener for when transaction is included in blockchain
        tx.getConfidence().addEventListener(new TransactionConfidence.Listener() {
            @Override
            public void onConfidenceChanged(Transaction tx, ChangeReason reason) {
                TransactionConfidence confidence = tx.getConfidence();
                if (reason.equals(ChangeReason.TYPE) && confidence.getConfidenceType().equals(TransactionConfidence.ConfidenceType.BUILDING)) {
                    Peer peer = kit.peerGroup().getConnectedPeers().get(0);
                    Sha256Hash blockHash = null;
                    //TODO: find better way to get hash of block where transaction is included
                    for (Sha256Hash hash : tx.getAppearsInHashes().keySet()) blockHash = hash;
                    registration.setBlockId(blockHash);
                    System.out.println("registration transaction included in block " + blockHash);
                    System.out.println("http://explorer.chain.com/blocks/" + blockHash);
                    try {
                        //download the full block where transaction was included
                        Block b = peer.getBlock(blockHash).get();
                        //generate peerId
                        Number160 peerId = generatePeerId(keyPair.getPublic(), b.getNonce());
                        registration.setPeerId(peerId);
                        registration.stopAsync();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        registration.awaitTerminated();
    }

    /**
     * Validates if the supplied peerId is based on public key in the transaction and the block nonce
     *
     * @return true if verification was successful
     */
    public boolean verify(Number160 peerId, Sha256Hash blockHash, Sha256Hash transactionHash) throws ExecutionException, InterruptedException, UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeySpecException {
        Peer peer = kit.peerGroup().getConnectedPeers().get(0);
        System.out.println("trying to get block " + blockHash);
        Block b = peer.getBlock(blockHash).get(); //blocking
        System.out.println("block nonce: " + b.getNonce());
        for(Transaction tx : b.getTransactions()) {
            System.out.println(tx.getHash());
            if (tx.getHash().equals(transactionHash)) {
                System.out.println("transaction found inside block");
                for(TransactionOutput output : tx.getOutputs()) {
                    if (output.getScriptPubKey().isOpReturn()) {
                        System.out.println(output.getScriptPubKey());
                        byte[] pubKeyEncoded = output.getScriptPubKey().getChunks().get(1).data;
                        System.out.println("found public key in transaction: " + net.tomp2p.utils.Utils.makeSHAHash(pubKeyEncoded));
                        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyEncoded);
                        KeyFactory keyFactory = KeyFactory.getInstance("DSA");
                        PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);
                        if (generatePeerId(pubKey, b.getNonce()).equals(peerId)) {
                            return true;
                        }
                    }
                }
                break;
            }
        }
        return false;
    }

    /**
     * Generates peerId by hashing the public key first, then appending the the block nonce and hash it again.
     *
     * @return generated peerId as Number160
     */
    public Number160 generatePeerId(PublicKey publicKey, Long blockNonce) {
        //TODO: come up with a better way to generate a nodeID form public key and block nonce
        // initialize peerId with SHAHash of public key
        Number160 peerId = Utils.makeSHAHash(publicKey.getEncoded());
        byte[] peerIdBytes = peerId.toByteArray();
        System.out.println("initial peerId: " + peerId);
        byte[] blockNonceBytes = ByteBuffer.allocate(8).putLong(blockNonce).array();
        // create a new array that is the size of the two arrays
        byte[] out = new byte[peerIdBytes.length + blockNonceBytes.length];
        // copy pub key into new array (from pos 0, copy pubKeyEnc.length bytes)
        System.arraycopy(peerIdBytes, 0, out, 0, peerIdBytes.length);
        // copy block nonce into end of new array (from pos pubKeyEnc.length, copy blockNonceBytes.length bytes)
        System.arraycopy(blockNonceBytes, 0, out, peerIdBytes.length, blockNonceBytes.length);
        // generate final peerId by hashing combined byte array
        peerId = Utils.makeSHAHash(out);
        System.out.println("block nonce: " + blockNonce + " (" + Hex.encodeHexString(blockNonceBytes) + ")");
        System.out.println("out: " + Hex.encodeHexString(out));
        System.out.println("generated peerId: " + peerId);
        return peerId;
    }

}
