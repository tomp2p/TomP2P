package net.tomp2p.bitcoin;

import net.tomp2p.peers.Number160;
import org.bitcoinj.params.TestNet3Params;
import org.junit.Ignore;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Random;

import static org.junit.Assert.*;

public class WalletServiceTest {
    private static WalletService ws;
    private static KeyPair keyPair;

    @org.junit.BeforeClass
    static public void setUp() throws Exception {
        ws = new WalletService(TestNet3Params.get(), "tomP2P-bitcoin");
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        keyPair = gen.generateKeyPair();
    }

    @org.junit.Test
    public void testStart() throws Exception {
        ws.start();
        System.out.println("Wallet balance: " + ws.kit.wallet().getBalance());
        assertTrue(ws.kit.peerGroup().getConnectedPeers().size() >= 3);
    }

    @org.junit.Test
    public void testRegisterNode() throws Exception {
        ws.registerNode(keyPair);
    }

    @org.junit.Test
    public void testVerify() throws Exception {
        Registration reg = ws.registration;
//        Values of a previous transaction:
//        Registration reg = new Registration(
//                new Number160("0x55f38ce1"),
//                new Sha256Hash("0000000000007d6ef50291d344a070ffa934fa5decfa771df373639ba4d3c068"),
//                new Sha256Hash("2e2e1a9fe348a7a85b26161cf2b839cf429d288796efe8da3515c227e875e2c6")
//        );
        assertTrue(ws.verify(reg.getPeerId(), reg.getBlockId(), reg.getTransactionId()));
        assertFalse(ws.verify(new Number160(new Random()), reg.getBlockId(), reg.getTransactionId()));
    }

    @org.junit.Test
    public void testGeneratePeerId() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair keyPair = gen.generateKeyPair();
        byte[] key = keyPair.getPublic().getEncoded();

        // loading public key from test resources
        URL resource = WalletServiceTest.class.getResource("/pubKey.key");
        Path path = Paths.get(resource.getPath());
        byte[] encKey = Files.readAllBytes(path);
        System.out.println("Reading public key from " + resource);
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(encKey);
        KeyFactory keyFactory = KeyFactory.getInstance("DSA");
        PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);
//        System.out.println(Arrays.toString(pubKey.getEncoded()));
        Long nonce = new Long("4032432432");
        Number160 peerId = ws.generatePeerId(pubKey, nonce);
        assertTrue(peerId.equals(new Number160("0xb223e7b712e8880775c237cf0009f5abd06c68bd")));
    }

    @org.junit.AfterClass
    static public void tearDown() {
        ws.stop();
    }
}